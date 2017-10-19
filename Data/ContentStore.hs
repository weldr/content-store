{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Data.ContentStore(ContentStore,
                         CsError(..),
                         CsMonad,
                         runCsMonad,
                         contentStoreValid,
                         fetchByteString,
                         fetchFile,
                         fetchLazyByteString,
                         mkContentStore,
                         openContentStore,
                         storeByteString,
                         storeByteStringC,
                         storeDirectory,
                         storeFile,
                         storeLazyByteString,
                         storeLazyByteStringC)
 where

import           Conduit((.|), Conduit, awaitForever, runConduit, sinkList, sourceDirectoryDeep, yield)
import           Control.Conditional(ifM, unlessM)
import           Control.Monad(forM, forM_)
import           Control.Monad.Base(MonadBase(..))
import           Control.Monad.Except(ExceptT, MonadError, catchError, runExceptT, throwError)
import           Control.Monad.IO.Class(MonadIO, liftIO)
import           Control.Monad.Trans.Class(lift)
import           Control.Monad.Trans.Control(MonadBaseControl(..))
import           Control.Monad.Trans.Resource(MonadResource, MonadThrow, ResourceT, runResourceT)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           System.Directory(canonicalizePath, copyFile, createDirectoryIfMissing, doesDirectoryExist, doesFileExist)
import           System.FilePath((</>))

import Data.ContentStore.Config(Config(..), defaultConfig, readConfig, writeConfig)
import Data.ContentStore.Digest

-- A ContentStore is its config file data and the base directory
-- where it is stored on disk.  This data type is opaque on purpose.
-- Users shouldn't concern themselves with the implementation of
-- a content store, just that it exists.
data ContentStore = ContentStore {
    csConfig :: Config,
    csRoot :: FilePath,
    csHash :: DigestAlgorithm
 }

data CsError = CsError String                       -- miscellaneous error
             | CsErrorCollision String              -- An object with this digest already exists
             | CsErrorConfig String                 -- A parse error occurred reading the config file
             | CsErrorInvalid String                -- The repo is invalid (probably, missing something)
             | CsErrorMissing                       -- The repo does not exist at all
             | CsErrorNoSuchObject String           -- No such object exists in the content store
             | CsErrorUnsupportedHash String        -- An unsupported hashing algorithm was used
 deriving (Eq, Show)

newtype CsMonad a = CsMonad { getCsMonad :: ResourceT (ExceptT CsError IO) a }
 deriving (Applicative, Functor, Monad, MonadBase IO, MonadError CsError, MonadIO, MonadResource, MonadThrow)

instance MonadBaseControl IO CsMonad where
    type StM CsMonad a = StM (ResourceT (ExceptT CsError IO)) a
    liftBaseWith f = CsMonad $ liftBaseWith $ \r -> f (r . getCsMonad)
    restoreM = CsMonad . restoreM

runCsMonad :: CsMonad a -> IO (Either CsError a)
runCsMonad x = runExceptT $ runResourceT $ getCsMonad x

csSubdirs :: [String]
csSubdirs = ["objects"]

--
-- PRIVATE FUNCTIONS
--

-- Objects are stored in the content store in a subdirectory
-- within the objects directory.  This function makes sure that
-- path exists.
ensureObjectSubdirectory :: ContentStore -> String -> IO ()
ensureObjectSubdirectory cs subdir =
    createDirectoryIfMissing True (objectSubdirectoryPath cs subdir)

-- Assemble the directory path where an object will be stored.
objectSubdirectoryPath :: ContentStore -> String -> FilePath
objectSubdirectoryPath ContentStore{..} subdir =
    csRoot </> "objects" </> subdir

-- Where in the content store should an object be stored?  This
-- function takes the calculated digest of the object and splits
-- it into a subdirectory and the filename within that directory.
--
-- This function is used when objects are on the way into the
-- content store.
storedObjectDestination :: ObjectDigest -> (String, String)
storedObjectDestination = storedObjectLocation . toHex

-- Where in the content store is an object stored?  This function
-- takes the digest of the object that we got from somewhere outside
-- of content store code and splits it into a subdirectory and the
-- filename within that directory.
--
-- This function is used when objects are on the way out of the
-- content store.
storedObjectLocation :: String -> (String, String)
storedObjectLocation = splitAt 2

-- Given a content store and a digest, try to find the file containing
-- that object.  This does not read the object off the disk.
findObject :: ContentStore -> ObjectDigest -> IO (Maybe FilePath)
findObject cs digest = do
    let (subdir, filename) = storedObjectDestination digest
        path               = objectSubdirectoryPath cs subdir </> filename

    ifM (doesFileExist path)
        (return $ Just path)
        (return Nothing)

doFetch :: (MonadError CsError m, MonadIO m) => ContentStore -> (FilePath -> IO a) -> ObjectDigest -> m a
doFetch cs reader digest =
    liftIO (findObject cs digest) >>= \case
        Nothing   -> throwError $ CsErrorNoSuchObject (toHex digest)
        Just path -> liftIO $ reader path

doStore :: (MonadError CsError m, MonadIO m) => ContentStore -> (a -> ObjectDigest) -> (FilePath -> a -> IO ()) -> a -> m ObjectDigest
doStore cs hasher writer object = do
    let digest             = hasher object
    let (subdir, filename) = storedObjectDestination digest
        path               = objectSubdirectoryPath cs subdir </> filename

    liftIO $ ensureObjectSubdirectory cs subdir

    -- Only store the object if it does not already exist in the content store.
    -- If it's already there, just return the digest.
    unlessM (liftIO $ doesFileExist path) $
        liftIO $ writer path object

    return digest

--
-- CONTENT STORE MANAGEMENT
--

-- Check that a content store exists and contains everything it's
-- supposed to.  This does not check the validity of all the contents.
-- That would be a lot of duplicated effort.
contentStoreValid :: (MonadError CsError m, MonadIO m) => FilePath -> m Bool
contentStoreValid fp = do
    unlessM (liftIO $ doesDirectoryExist fp) $
        throwError CsErrorMissing

    unlessM (liftIO $ doesFileExist $ fp </> "config") $
        throwError $ CsErrorInvalid "config"

    forM_ csSubdirs $ \subdir ->
        unlessM (liftIO $ doesDirectoryExist $ fp </> subdir) $
            throwError $ CsErrorInvalid subdir

    return True

-- Create a new content store on disk, rooted at the path given.
-- Return the ContentStore record.
--
-- Lots to think about in this function.  What does error handling
-- look like here (and everywhere else)?  There's lots of things
-- that could go wrong creating a store on disk.  Maybe we should
-- thrown exceptions or do something besides just returning a
-- Maybe.
mkContentStore :: (MonadError CsError m, MonadIO m) => FilePath -> m ContentStore
mkContentStore fp = do
    path <- liftIO $ canonicalizePath fp

    csExists <- contentStoreValid path `catchError` \_ -> return False
    if csExists then openContentStore path
    else do
        -- Create the required subdirectories.
        mapM_ (\d -> liftIO $ createDirectoryIfMissing True (path </> d))
              csSubdirs

        -- Write a config file.
        liftIO $ writeConfig (path </> "config") defaultConfig

        openContentStore path

-- Return an already existing content store.
--
-- There's a lot to think about here, too.  All the same error
-- handling questions still apply.  What happens if someone is
-- screwing around with the directory at the same time this code
-- is running?  Do we need to lock it somehow?
openContentStore :: (MonadError CsError m, MonadIO m) => FilePath -> m ContentStore
openContentStore fp = do
    path <- liftIO $ canonicalizePath fp

    _ <- contentStoreValid path

    conf <- liftIO (readConfig $ path </> "config") >>= \case
        Left e  -> throwError $ CsErrorConfig (show e)
        Right c -> return c

    let algo = confHash conf

    case getDigestAlgorithm algo of
        Nothing -> throwError $ CsErrorUnsupportedHash (show algo)
        Just da -> return ContentStore { csRoot=path, csConfig=conf, csHash=da }

--
-- STRICT BYTE STRING INTERFACE
--

-- Given an ObjectDigest for an object in the content store, load it into
-- a ByteString. Note that you'll probably need to use fromByteString to
-- produce an ObjectDigest from whatever text or binary representation
-- you've got from the user/mddb/etc.
fetchByteString :: (MonadError CsError m, MonadIO m) => ContentStore -> ObjectDigest -> m BS.ByteString
fetchByteString cs = doFetch cs BS.readFile

-- Given an object as a ByteString, put it into the content store.  Return the
-- object's hash so it can be recorded elsewhere.  If an object with the same
-- hash already exists in the content store, this is a duplicate.  Simply
-- return the hash of the already stored object.
storeByteString :: (MonadError CsError m, MonadIO m) => ContentStore -> BS.ByteString -> m ObjectDigest
storeByteString cs = doStore cs (digestByteString $ csHash cs) BS.writeFile

-- Given a Conduit of ByteStrings, store each one into the content store and put
-- the hash of each into the Conduit.  This is useful for storing many objects
-- at a time, like when importing an RPM or other package.  If an object with the
-- same hash already exists in the content store, this is a duplicate.  Simply
-- return the hash of the already stored object.
storeByteStringC :: (MonadError CsError m, MonadIO m) => ContentStore -> Conduit BS.ByteString m ObjectDigest
storeByteStringC cs = awaitForever $ \bs -> do
    digest <- lift $ storeByteString cs bs
    yield digest

--
-- LAZY BYTE STRING INTERFACE
--

-- Like fetchByteString, but uses lazy ByteStrings instead.
fetchLazyByteString :: (MonadError CsError m, MonadIO m) => ContentStore -> ObjectDigest -> m LBS.ByteString
fetchLazyByteString cs = doFetch cs LBS.readFile

-- Like storeByteString, but uses lazy ByteStrings instead.
storeLazyByteString :: (MonadError CsError m, MonadIO m) => ContentStore -> LBS.ByteString -> m ObjectDigest
storeLazyByteString cs = doStore cs (digestLazyByteString $ csHash cs) LBS.writeFile

-- Like storeByteStringC, but uses lazy ByteStrings instead.
storeLazyByteStringC :: (MonadError CsError m, MonadIO m) => ContentStore -> Conduit LBS.ByteString m ObjectDigest
storeLazyByteStringC cs = awaitForever $ \bs -> do
    digest <- lift $ storeLazyByteString cs bs
    yield digest

--
-- DIRECTORY INTERFACE
--

storeDirectory :: (MonadResource m, MonadError CsError m, MonadIO m) => ContentStore -> FilePath -> m [(FilePath, ObjectDigest)]
storeDirectory cs fp = do
    let hasher = digestByteString $ csHash cs

    entries <- runConduit $ sourceDirectoryDeep False fp .| sinkList
    forM entries $ \entry -> do
        object <- liftIO $ BS.readFile entry
        digest <- doStore cs hasher BS.writeFile object
        return (entry, digest)

--
-- FILE INTERFACE
--

fetchFile :: ContentStore -> ObjectDigest -> FilePath -> CsMonad ()
fetchFile cs digest dest =
    liftIO (findObject cs digest) >>= \case
        Nothing   -> throwError $ CsErrorNoSuchObject (toHex digest)
        Just path -> liftIO $ copyFile path dest

storeFile :: ContentStore -> FilePath -> CsMonad ObjectDigest
storeFile cs fp = do
    lbs <- liftIO $ LBS.readFile fp
    storeLazyByteString cs lbs
