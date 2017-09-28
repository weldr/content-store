{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Data.ContentStore(ContentStore,
                         contentStoreValid,
                         fetchByteString,
                         fetchLazyByteString,
                         mkContentStore,
                         openContentStore,
                         storeByteString,
                         storeLazyByteString)
 where

import           Control.Conditional(ifM, unlessM)
import           Control.Monad(forM_)
import           Control.Monad.Except(MonadError, catchError, throwError)
import           Control.Monad.IO.Class(MonadIO, liftIO)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import           System.Directory(canonicalizePath, createDirectoryIfMissing, doesDirectoryExist, doesFileExist)
import           System.FilePath((</>))

import Data.ContentStore.Config(Config(..), defaultConfig, readConfig, writeConfig)
import Data.ContentStore.Digest(ObjectDigest(..), hashByteString, hashLazyByteString)

-- A ContentStore is its config file data and the base directory
-- where it is stored on disk.  This data type is opaque on purpose.
-- Users shouldn't concern themselves with the implementation of
-- a content store, just that it exists.
data ContentStore = ContentStore {
    csConfig :: Config,
    csRoot :: FilePath
 }

data CsError = CsErrorCollision String
             | CsErrorConfig String
             | CsErrorInvalid String
             | CsErrorMissing
             | CsErrorUnsupportedHash String

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
storedObjectDestination (ObjectSHA256 digest) = splitAt 2 (show digest)
storedObjectDestination (ObjectSHA512 digest) = splitAt 2 (show digest)

-- Where in the content store is an object stored?  This function
-- takes the digest of the object that we got from somewhere outside
-- of content store code and splits it into a subdirectory and the
-- filename within that directory.
--
-- This function is used when objects are on the way out of the
-- content store.
storedObjectLocation :: String -> (String, String)
storedObjectLocation = splitAt 2

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

    forM_ ["objects"] $ \subdir ->
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
              ["objects"]

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
    liftIO (readConfig (path </> "config")) >>= \case
        Left e  -> throwError $ CsErrorConfig (show e)
        Right c -> return ContentStore { csConfig=c,
                                         csRoot=path }

--
-- STRICT BYTE STRING INTERFACE
--

-- Given the hash to an object in the content store, load it into
-- a ByteString.  Here, the hash is a string because it is assumed
-- it's coming from the mddb which doesn't know about various digest
-- algorithms.
fetchByteString :: ContentStore -> String -> IO (Maybe BS.ByteString)
fetchByteString cs digest = do
    let (subdir, filename) = storedObjectLocation digest
        path               = objectSubdirectoryPath cs subdir </> filename

    ifM (doesFileExist path)
        (Just <$> BS.readFile path)
        (return Nothing)

-- Given an object as a ByteString, put it into the content store.
-- Return the object's hash so it can be recorded elsewhere.
storeByteString :: (MonadError CsError m, MonadIO m) => ContentStore -> BS.ByteString -> m ObjectDigest
storeByteString cs bs = do
    let algo = confHash . csConfig $ cs

    case hashByteString algo bs of
        Nothing     -> throwError $ CsErrorUnsupportedHash (T.unpack algo)
        Just digest -> do
            let (subdir, filename) = storedObjectDestination digest
                path               = objectSubdirectoryPath cs subdir </> filename

            liftIO $ ensureObjectSubdirectory cs subdir

            ifM (liftIO $ doesFileExist path)
                (throwError $ CsErrorCollision path)
                (liftIO $ BS.writeFile path bs)

            return digest

--
-- LAZY BYTE STRING INTERFACE
--

-- Like fetchByteString, but uses lazy ByteStrings instead.
fetchLazyByteString :: ContentStore -> String -> IO (Maybe LBS.ByteString)
fetchLazyByteString cs digest = do
    let (subdir, filename) = storedObjectLocation digest
        path               = objectSubdirectoryPath cs subdir </> filename

    ifM (doesFileExist path)
        (Just <$> LBS.readFile path)
        (return Nothing)

-- Like storeByteString, but uses lazy ByteStrings instead.
storeLazyByteString :: (MonadError CsError m, MonadIO m) => ContentStore -> LBS.ByteString -> m ObjectDigest
storeLazyByteString cs lbs = do
    let algo = confHash . csConfig $ cs

    case hashLazyByteString algo lbs of
        Nothing     -> throwError $ CsErrorUnsupportedHash (T.unpack algo)
        Just digest -> do
            let (subdir, filename) = storedObjectDestination digest
                path               = objectSubdirectoryPath cs subdir </> filename

            liftIO $ ensureObjectSubdirectory cs subdir

            ifM (liftIO $ doesFileExist path)
                (throwError $ CsErrorCollision path)
                (liftIO $ LBS.writeFile path lbs)

            return digest
