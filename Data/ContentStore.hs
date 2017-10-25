{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- |
-- Module: Data.ContentStore
-- Copyright: (c) 2017 Red Hat, Inc.
-- License: LGPL
--
-- Maintainer: https://github.com/weldr
-- Stability: alpha
-- Portability: portable
--
-- Create content stores, put objects into them, and retrieve objects from them.

module Data.ContentStore(ContentStore,
                         CsError(..),
                         CsMonad,
                         runCsMonad,
                         contentStoreValid,
                         fetchByteString,
                         fetchByteStringC,
                         fetchFile,
                         fetchLazyByteString,
                         fetchLazyByteStringC,
                         mkContentStore,
                         openContentStore,
                         storeByteString,
                         storeByteStringC,
                         storeByteStringSink,
                         storeDirectory,
                         storeFile,
                         storeLazyByteString,
                         storeLazyByteStringC,
                         storeLazyByteStringSink)
 where

import           Conduit((.|), Conduit, Sink, await, awaitForever, runConduit, sinkLazy, sinkList, sourceDirectoryDeep, sourceFile, yield)
import           Control.Conditional(ifM, unlessM, whenM)
import           Control.Monad((>=>), forM, forM_, void)
import           Control.Monad.Base(MonadBase(..))
import           Control.Monad.Except(ExceptT, MonadError, catchError, runExceptT, throwError)
import           Control.Monad.IO.Class(MonadIO, liftIO)
import           Control.Monad.Trans.Class(lift)
import           Control.Monad.Trans.Control(MonadBaseControl(..))
import           Control.Monad.Trans.Resource(MonadResource, MonadThrow, ResourceT, runResourceT)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.Maybe(isNothing)
import           System.Directory(canonicalizePath, copyFile, createDirectoryIfMissing, doesDirectoryExist, doesFileExist, listDirectory, removeFile, renameFile)
import           System.FilePath((</>))
import           System.IO(Handle, SeekMode(..))
import           System.IO.Temp(openTempFile)
import           System.Posix.IO(FileLock, LockRequest(..), OpenMode(..), closeFd, defaultFileFlags, fdToHandle, getLock, handleToFd, openFd, setLock, waitToSetLock)

import Data.ContentStore.Config(Config(..), defaultConfig, readConfig, writeConfig)
import Data.ContentStore.Digest

-- | The ContentStore is an opaque type that contains various pieces of information
-- used to describe an on-disk content store.  Values of this type are constructed
-- via 'mkContentStore' and 'openContentStore', depending on which operation you
-- need to perform.  Users should not need to concern themselves with the internals
-- of this type.
data ContentStore = ContentStore {
    csConfig :: Config,
    csRoot :: FilePath,
    csHash :: DigestAlgorithm
 }

-- | A type to represent various errors that can occur during content store operations.
data CsError =
    -- | Miscellaneous, uncategorized errors.  The string is the exact error message.
    CsError String
    -- | An object with this digest already exists in the content store.  This error
    -- is not typically raised during write operations, because attempting to write
    -- the same thing twice is not really an error.
  | CsErrorCollision String
    -- | A parse error occurred reading the content store's internal config file.  This
    -- generally represents either a programming error or that someone has been modifying
    -- the internals.  The string contains the error message.
  | CsErrorConfig String
    -- | The content store directory is invalid.  This usually occurs because some file
    -- or directory is missing.  The string is the name of what is missing.
  | CsErrorInvalid String
    -- | The content store does not exist.
  | CsErrorMissing
    -- | The requested object does not exist in the content store.  The string contains
    -- the object digest requested.
  | CsErrorNoSuchObject String
    -- | The hashing algorithm is not supported by the content store.  Not all possible
    -- algorithms are supported.  The string contains the name of the algorithm requested.
  | CsErrorUnsupportedHash String
 deriving (Eq, Show)

-- | Working with a 'ContentStore' requires a lot of behind-the-scenes management of
-- 'ResourceT', 'ExceptT', and 'IO'.  Along with 'runCsMonad', this provides a type
-- and function for doing much of that management for you.  These two can be used like
-- so:
--
-- > result <- runCsMonad $ do
-- >     cs <- mkContentStore "/tmp/cs.repo"
-- >     storeDirectory cs "/tmp/test-data"
-- >
-- > case result of
-- >     Left e  -> print e
-- >     Right d -> do putStrLn "Stored objects: "
-- >                   mapM_ print d
--
-- Most functions in this module do not explicitly require use of 'CsMonad', but any
-- that have a return type including 'm' can be run inside it.
newtype CsMonad a = CsMonad { getCsMonad :: ResourceT (ExceptT CsError IO) a }
 deriving (Applicative, Functor, Monad, MonadBase IO, MonadError CsError, MonadIO, MonadResource, MonadThrow)

instance MonadBaseControl IO CsMonad where
    type StM CsMonad a = StM (ResourceT (ExceptT CsError IO)) a
    liftBaseWith f = CsMonad $ liftBaseWith $ \r -> f (r . getCsMonad)
    restoreM = CsMonad . restoreM

-- | See the documentation for 'CsMonad'.
runCsMonad :: CsMonad a -> IO (Either CsError a)
runCsMonad x = runExceptT $ runResourceT $ getCsMonad x

csSubdirs :: [String]
csSubdirs = ["objects", "tmp", "lock"]

--
-- PRIVATE FUNCTIONS
--

-- Objects are stored in the content store in a subdirectory within the objects directory.
-- This function makes sure that path exists.
ensureObjectSubdirectory :: ContentStore -> String -> IO ()
ensureObjectSubdirectory cs subdir =
    createDirectoryIfMissing True (objectSubdirectoryPath cs subdir)

-- Assemble the directory path where an object will be stored.
objectSubdirectoryPath :: ContentStore -> String -> FilePath
objectSubdirectoryPath ContentStore{..} subdir =
    csRoot </> "objects" </> subdir

-- Where in the content store should an object be stored?  This function takes the calculated
-- digest of the object and splits it into a subdirectory and the filename within that
-- directory.
--
-- This function is used when objects are on the way into the content store.
storedObjectDestination :: ObjectDigest -> (String, String)
storedObjectDestination = storedObjectLocation . toHex

-- Where in the content store is an object stored?  This function takes the digest of the object
-- that we got from somewhere outside of content store code and splits it into a subdirectory
-- and the filename within that directory.
--
-- This function is used when objects are on the way out of the content store.
storedObjectLocation :: String -> (String, String)
storedObjectLocation = splitAt 2

-- Given a content store and a digest, try to find the file containing that object.  This
-- does not read the object off the disk.
findObject :: (MonadError CsError m, MonadIO m) => ContentStore -> ObjectDigest -> m FilePath
findObject cs digest = do
    let (subdir, filename) = storedObjectDestination digest
        path               = objectSubdirectoryPath cs subdir </> filename

    ifM (liftIO $ doesFileExist path)
        (return path)
        (throwError $ CsErrorNoSuchObject $ toHex digest)

startStore :: ContentStore -> IO (FilePath, Handle)
startStore ContentStore{..} = do
    -- Acquire the global lock to prevent a race between creating the tmp file and locking it.
    (path, fd) <- withGlobalLock csRoot $ do
        -- Create a new file in the tmp directory
        (path, handle) <- openTempFile (csRoot </> "tmp") "import"

        -- NB: this step closes handle
        fd <- handleToFd handle

        -- Lock the file
        setLock fd fullLock

        return (path, fd)

    -- Reopen the locked fd as a handle and return
    handle' <- fdToHandle fd
    return (path, handle')

finishStore :: ContentStore -> (FilePath, Handle) -> ObjectDigest -> IO ()
finishStore cs (tmpPath, handle) digest = do
    let (subdir, filename) = storedObjectDestination digest
    let path               = objectSubdirectoryPath cs subdir </> filename

    ensureObjectSubdirectory cs subdir

    -- Move the file into the object directory
    renameFile tmpPath path

    -- Unlock the file and close the descriptor
    fd <- handleToFd handle
    setLock fd fullUnlock
    closeFd fd

-- This stores an object that is already (or can be) fully loaded into memory
doStore :: MonadIO m => ContentStore -> (a -> ObjectDigest) -> (Handle -> a -> IO ()) -> a -> m ObjectDigest
doStore cs hasher writer object = do
    let digest             = hasher object
    let (subdir, filename) = storedObjectDestination digest
        path               = objectSubdirectoryPath cs subdir </> filename

    liftIO $ ensureObjectSubdirectory cs subdir

    -- Only store the object if it does not already exist in the content store.
    -- If it's already there, just return the digest.
    liftIO $ unlessM (doesFileExist path) $ do
        (tmpPath, handle) <- startStore cs
        writer handle object
        finishStore cs (tmpPath, handle) digest

    return digest

-- Stream data into the content-store without loading all of it at once
doStoreSink :: MonadIO m => ContentStore -> (DigestContext -> a -> DigestContext) -> (Handle -> a -> IO ()) -> Sink a m ObjectDigest
doStoreSink cs hasher writer = do
    (tmpPath, handle) <- liftIO $ startStore cs
    let initctx = digestInit $ csHash cs
    digest <- doStream (tmpPath, handle) initctx
    let (subdir, _) = storedObjectDestination digest

    liftIO $ ensureObjectSubdirectory cs subdir
    liftIO $ finishStore cs (tmpPath, handle) digest

    return digest
 where
    doStream (tmpPath, handle) ctx = await >>= \case
        Nothing    -> return $ digestFinalize ctx
        Just chunk -> do
            let newctx = hasher ctx chunk
            liftIO $ writer handle chunk
            doStream (tmpPath, handle) newctx

-- lock file management
fullLock :: FileLock
fullLock = (WriteLock, AbsoluteSeek, 0, 0)

fullUnlock :: FileLock
fullUnlock = (Unlock, AbsoluteSeek, 0, 0)

withGlobalLock :: MonadIO m => FilePath -> m a -> m a
withGlobalLock csRoot action = do
    let lockFile = csRoot </> "lock" </> "lockfile"

    -- Create or open the lock file
    fd <- liftIO $ openFd lockFile WriteOnly (Just 0o644) defaultFileFlags

    -- Acquire the lock
    liftIO $ waitToSetLock fd fullLock

    -- Perform the action
    ret <- action

    -- Release the lock
    liftIO $ setLock fd fullUnlock

    return ret

-- Cleanup any stale tmp files. These are any files in the tmp directory
-- that are not locked, while the global lock is held.
cleanupTmp :: FilePath -> IO ()
cleanupTmp csRoot = withGlobalLock csRoot $ listDirectory (csRoot </> "tmp") >>= mapM_ cleanupOne
 where
    cleanupOne :: FilePath -> IO ()
    cleanupOne tmpFile = do
        let fullPath = csRoot </> tmpFile
        fd <- openFd fullPath ReadOnly Nothing defaultFileFlags
        whenM (isNothing <$> getLock fd fullLock) $ removeFile fullPath

--
-- PUBLIC FUNCTIONS
--

--
-- CONTENT STORE MANAGEMENT
--

-- | Check that a content store exists and contains everything it's supposed to.  This does
-- not check the validity of all the contents, however.  A 'CsError' will be thrown if there
-- are any problems.  Otherwise, this function returns True.
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

-- | Create a new 'ContentStore' on disk, rooted at the path given, and return it as if
-- 'openContentStore' had also been called.  If a content store already exists at the
-- given root and is valid, return that as if 'openContentStore' had been called.
-- Various 'CsError's could be thrown by this process.
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

-- | Return an already existing 'ContentStore', after checking that it is valid.
-- Various 'CsError's could be thrown by this process.
openContentStore :: (MonadError CsError m, MonadIO m) => FilePath -> m ContentStore
openContentStore fp = do
    path <- liftIO $ canonicalizePath fp

    void $ contentStoreValid path

    liftIO $ cleanupTmp path

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

-- | Lookup and return some previously stored object as a strict 'ByteString'.  Note that
-- you'll probably need to use 'fromByteString' to produce an 'ObjectDigest' from whatever
-- text or binary representation you've got from the user/mddb/etc.
fetchByteString :: (MonadError CsError m, MonadIO m) =>
                   ContentStore     -- ^ An opened 'ContentStore'.
                -> ObjectDigest     -- ^ The 'ObjectDigest' for some stored object.
                -> m BS.ByteString
fetchByteString cs digest = findObject cs digest >>= liftIO . BS.readFile

-- | Given an opened 'ContentStore' and a 'Conduit' of 'ObjectDigest's, load each one into
-- a strict 'ByteString' and put it into the conduit.  This is useful for stream many
-- objects out of the content store at a time, like with exporting an RPM or other package
-- format.
fetchByteStringC :: (MonadError CsError m, MonadIO m, MonadResource m) => ContentStore -> Conduit ObjectDigest m BS.ByteString
fetchByteStringC cs = awaitForever $
    findObject cs >=> sourceFile

-- | Store some object into the content store and return its 'ObjectDigest'.  If an object
-- with the same digest already exists in the content store, this is a duplicate.  Simply
-- return the digest of the already stored object and do nothing else.  A 'CsErrorCollision'
-- will NOT be thrown.
storeByteString :: (MonadError CsError m, MonadIO m) =>
                   ContentStore     -- ^ An opened 'ContentStore'.
                -> BS.ByteString    -- ^ An object to be stored, as a strict 'ByteString'.
                -> m ObjectDigest
storeByteString cs = doStore cs (digestByteString $ csHash cs) BS.hPut

-- | Like 'storeByteString', but read strict 'ByteString's from a 'Conduit' and put their
-- 'ObjectDigest's into the conduit.  This is useful for storing many objects at a time,
-- like with importing an RPM or other package format.
storeByteStringC :: (MonadError CsError m, MonadIO m) => ContentStore -> Conduit BS.ByteString m ObjectDigest
storeByteStringC cs = awaitForever $ \bs -> do
    digest <- lift $ storeByteString cs bs
    yield digest

-- | Read in a 'Conduit' of strict 'ByteString's, store the stream into an object in an
-- already opened 'ContentStore', and return the final digest.  This is useful for
-- storing a stream of data as a single object.
storeByteStringSink :: MonadIO m => ContentStore -> Sink BS.ByteString m ObjectDigest
storeByteStringSink cs = doStoreSink cs digestUpdate BS.hPut

--
-- LAZY BYTE STRING INTERFACE
--

-- | Like 'fetchByteString', but uses lazy 'Data.ByteString.Lazy.ByteString's instead.
fetchLazyByteString :: (MonadError CsError m, MonadIO m) => ContentStore -> ObjectDigest -> m LBS.ByteString
fetchLazyByteString cs digest = findObject cs digest >>= liftIO . LBS.readFile

-- | Like 'fetchByteStringC', but uses lazy 'Data.ByteString.Lazy.ByteString's instead.
fetchLazyByteStringC :: (MonadError CsError m, MonadIO m, MonadResource m) => ContentStore -> Conduit ObjectDigest m LBS.ByteString
fetchLazyByteStringC cs = awaitForever $
    findObject cs >=> \path -> sourceFile path .| sinkLazy

-- | Like 'storeByteString', but uses lazy 'Data.ByteString.Lazy.ByteString's instead.
storeLazyByteString :: (MonadError CsError m, MonadIO m) => ContentStore -> LBS.ByteString -> m ObjectDigest
storeLazyByteString cs = doStore cs (digestLazyByteString $ csHash cs) LBS.hPut

-- | Like 'storeByteStringC', but uses lazy 'Data.ByteString.Lazy.ByteString's instead.
storeLazyByteStringC :: (MonadError CsError m, MonadIO m) => ContentStore -> Conduit LBS.ByteString m ObjectDigest
storeLazyByteStringC cs = awaitForever $ \bs -> do
    digest <- lift $ storeLazyByteString cs bs
    yield digest

-- | Like 'storeByteStringSink', but uses lazy 'Data.ByteString.Lazy.ByteString's instead.
storeLazyByteStringSink :: MonadIO m => ContentStore -> Sink LBS.ByteString m ObjectDigest
storeLazyByteStringSink cs = doStoreSink cs (LBS.foldlChunks digestUpdate) LBS.hPut

--
-- DIRECTORY INTERFACE
--

-- | Store all objects in a directory tree in the content store, returning the name and 'ObjectDigest'
-- of each.  Note that directories will not be stored.  The content store only contains things that
-- have content.  If you need to store directory information, that should be handled externally to
-- this module.
storeDirectory :: (MonadResource m, MonadError CsError m, MonadIO m) =>
                  ContentStore                  -- ^ An opened 'ContentStore'.
               -> FilePath                      -- ^ A directory tree containing many objects.
               -> m [(FilePath, ObjectDigest)]
storeDirectory cs fp = do
    let hasher = digestByteString $ csHash cs

    entries <- runConduit $ sourceDirectoryDeep False fp .| sinkList
    forM entries $ \entry -> do
        object <- liftIO $ BS.readFile entry
        digest <- doStore cs hasher BS.hPut object
        return (entry, digest)

--
-- FILE INTERFACE
--

-- | Find some object in the content store and write it to a destination.  If the destination already
-- exists, it will be overwritten.  If the object does not already exist, a 'CsErrorNoSuchObject' will
-- be thrown.
fetchFile :: (MonadResource m, MonadError CsError m, MonadIO m) =>
             ContentStore   -- ^ An opened 'ContentStore'.
          -> ObjectDigest   -- ^ The 'ObjectDigest' of some stored object.
          -> FilePath       -- ^ The destination
          -> m ()
fetchFile cs digest dest = findObject cs digest >>= \path -> liftIO $ copyFile path dest

-- | Store an already existing file in the content store, returning its 'ObjectDigest'.  The original
-- file will be left on disk.
storeFile :: (MonadResource m, MonadError CsError m, MonadIO m) =>
             ContentStore           -- ^ An opened 'ContentStore'.
          -> FilePath               -- ^ The file to be stored.
          -> m ObjectDigest
storeFile cs fp = do
    lbs <- liftIO $ LBS.readFile fp
    storeLazyByteString cs lbs
