{-# LANGUAGE CPP #-}

module Data.ContentStoreSpec(spec)
 where

import           Conduit((.|), ConduitM, runConduitRes, sinkList, yield, yieldMany)
import           Control.Monad.Except(ExceptT, runExceptT)
import           Control.Monad.Trans.Resource(ResourceT, runResourceT)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.Either(isRight)
import           Data.Maybe(fromJust)
import           Data.Void(Void)
import           System.Directory(createDirectory, doesFileExist, getTemporaryDirectory, removeFile)
import           System.FilePath.Posix((</>))
import           System.IO(Handle, hClose)
import           System.IO.Temp(openTempFile, withSystemTempDirectory)
import           Test.Hspec

import Data.ContentStore
import Data.ContentStore.Digest

{-# ANN module ("HLint: ignore Eta reduce" :: String) #-}
{-# ANN module ("HLint: ignore Reduce duplication" :: String) #-}

invalidChecksum :: BS.ByteString
invalidChecksum = "\x82\xe4\x69\x8a\x1e\xbe\x82\xed\xae\xc8\xe8\xa6\xdc\xf1\x8d\x57\xbb\x5a\xc6\x71\x8b\x6b\xf2\xbb\xc3\x8a\x00\xda\x2e\x29\x34\xe6"

anyDigest :: Either CsError ObjectDigest -> Bool
anyDigest = isRight

openSystemTempFile :: String -> IO (FilePath, Handle)
openSystemTempFile template = do
    dir <- getTemporaryDirectory
    openTempFile dir template

runCs :: ResourceT (ExceptT CsError IO) a -> IO (Either CsError a)
runCs = runExceptT . runResourceT

runCsConduit :: ConduitM () Void (ResourceT (ExceptT CsError IO)) a -> IO (Either CsError a)
runCsConduit = runExceptT . runConduitRes

withContentStore :: ActionWith ContentStore -> IO ()
withContentStore action = withTempDir $ \d ->
    runExceptT (mkContentStore d) >>= either (expectationFailure . show) action

withStoredFile :: FilePath -> ActionWith (ContentStore, ObjectDigest) -> IO ()
withStoredFile f action =
    withContentStore $ \cs ->
        runCs (storeFile cs f) >>= \case
            Left e       -> expectationFailure $ show e
            Right digest -> action (cs, digest)

withTempDir :: ActionWith FilePath -> IO ()
withTempDir action =
    withSystemTempDirectory "cs.repo" action

contentStoreValidSpec :: Spec
contentStoreValidSpec =
    describe "contentStoreValid" $ do
        it "raises CsErrorMissing when directory doesn't exist" $
            runExceptT (contentStoreValid "") >>= (`shouldBe` Left CsErrorMissing)

        around withTempDir $
            it "raises CsErrorInvalid when config file doesn't exist" $ \repo ->
                runExceptT (contentStoreValid repo) >>= (`shouldBe` Left (CsErrorInvalid "config"))

        around withTempDir $
            it "raises CsErrorInvalid with objects subdir doesn't exist" $ \repo -> do
                appendFile (repo </> "config") ""
                runExceptT (contentStoreValid repo) >>= (`shouldBe` Left (CsErrorInvalid "objects"))

        around withTempDir $
            it "raises CsErrorInvalid with tmp subdir doesn't exist" $ \repo -> do
                appendFile (repo </> "config") ""
                createDirectory $ repo </> "objects"
                runExceptT (contentStoreValid repo) >>= (`shouldBe` Left (CsErrorInvalid "tmp"))

        around withTempDir $
            it "raises CsErrorInvalid with lock subdir doesn't exist" $ \repo -> do
                appendFile (repo </> "config") ""
                createDirectory $ repo </> "objects"
                createDirectory $ repo </> "tmp"
                runExceptT (contentStoreValid repo) >>= (`shouldBe` Left (CsErrorInvalid "lock"))

        around withTempDir $
            it "repo should be valid" $ \repo -> do
                appendFile (repo </> "config") ""
                createDirectory $ repo </> "objects"
                createDirectory $ repo </> "tmp"
                createDirectory $ repo </> "lock"
                runExceptT (contentStoreValid repo) >>= (`shouldBe` Right True)

contentStoreDigestSpec :: Spec
contentStoreDigestSpec =
    describe "contentStoreDigest" $
        around withContentStore $
            it "default digest is BLAKE2b256" $ \cs ->
                -- The constructors of DigestAlgorithm are not exported, so we can only
                -- compare names.
                digestName (contentStoreDigest cs) `shouldBe` "Blake2b_256"

fetchByteStringSpec :: Spec
fetchByteStringSpec =
    describe "fetchByteString" $ do
        around withContentStore $
            it "raises CsErrorNoSuchObject when the object doesn't exist" $ \cs -> do
                let od = fromJust $ fromByteString (contentStoreDigest cs) invalidChecksum

                runCs (fetchByteString cs od) >>= (`shouldBe` Left (CsErrorNoSuchObject $ toHex od))

        around (withStoredFile __FILE__) $
            it "stored file and fetched file are the same" $ \(cs, digest) ->
                runCs (fetchByteString cs digest) >>= \case
                    Left e    -> expectationFailure (show e)
                    Right bs2 -> do
                        bs1 <- BS.readFile __FILE__
                        bs2 `shouldBe` bs1

fetchByteStringCSpec :: Spec
fetchByteStringCSpec =
    describe "fetchByteStringC" $ do
        around withContentStore $
            it "raises CsErrorNoSuchObject when the object doesn't exist" $ \cs -> do
                let od = fromJust $ fromByteString (contentStoreDigest cs) invalidChecksum

                runCsConduit (yield od .| fetchByteStringC cs .| sinkList) >>= (`shouldBe` Left (CsErrorNoSuchObject $ toHex od))

        around (withStoredFile __FILE__) $
            it "stored file and fetched file are the same" $ \(cs, digest) ->
                runCsConduit (yield digest .| fetchByteStringC cs .| sinkList) >>= \case
                    Right [bs2] -> do
                        bs1 <- BS.readFile __FILE__
                        bs2 `shouldBe` bs1
                    Right l -> expectationFailure $ "fetchByteStringC returned " ++ show (length l) ++ " elements"
                    Left e  -> expectationFailure (show e)

        around (withStoredFile __FILE__) $
            it "requesting two digests returns two files" $ \(cs, digest) ->
                runCsConduit (yieldMany [digest, digest] .| fetchByteStringC cs .| sinkList) >>= \case
                    Left e    -> expectationFailure (show e)
                    Right lst -> length lst `shouldBe` 2

storeByteStringSpec :: Spec
storeByteStringSpec =
    describe "storeByteString" $ do
        around withContentStore $
            it "storing a bytestring should return a digest" $ \cs ->
                runCs (storeByteString cs "The spice must flow.") >>= (`shouldSatisfy` anyDigest)

        around withContentStore $
            it "duplicate stores do not raise an error" $ \cs -> do
                first  <- runCs $ storeByteString cs "blahblahblah"
                second <- runCs $ storeByteString cs "blahblahblah"

                first `shouldSatisfy` anyDigest
                first `shouldBe` second

storeByteStringCSpec :: Spec
storeByteStringCSpec =
    describe "storeByteStringC" $ do
        around withContentStore $
            it "storing a bytestring should return a digest" $ \cs ->
                runCsConduit (yield "The spice must flow." .| storeByteStringC cs .| sinkList) >>= \case
                    Left e    -> expectationFailure (show e)
                    Right lst -> length lst `shouldBe` 1

        around withContentStore $
            it "storing multiple bytestrings should return multiple digests" $ \cs -> do
                let strs = ["I must not fear.",
                            "Fear is the mind-killer.",
                            "Fear is the little-death that brings total obliteration."]
                runCsConduit (yieldMany strs .| storeByteStringC cs .| sinkList) >>= \case
                    Left e    -> expectationFailure (show e)
                    Right lst -> length lst `shouldBe` 3

storeByteStringSinkSpec :: Spec
storeByteStringSinkSpec =
    describe "storeByteStringSink" $
        around withContentStore $
            it "storing multiple bytestrings should return a single digest" $ \cs -> do
                let strs = ["I must not fear.",
                            "Fear is the mind-killer.",
                            "Fear is the little-death that brings total obliteration."]
                runCsConduit (yieldMany strs .| storeByteStringSink cs) >>= (`shouldSatisfy` anyDigest)

fetchLazyByteStringSpec :: Spec
fetchLazyByteStringSpec =
    describe "fetchLazyByteString" $ do
        around withContentStore $
            it "raises CsErrorNoSuchObject when the object doesn't exist" $ \cs -> do
                let od = fromJust $ fromByteString (contentStoreDigest cs) invalidChecksum

                runCs (fetchLazyByteString cs od) >>= (`shouldBe` Left (CsErrorNoSuchObject $ toHex od))

        around (withStoredFile __FILE__) $
            it "stored file and fetched file are the same" $ \(cs, digest) ->
                runCs (fetchLazyByteString cs digest) >>= \case
                    Left e    -> expectationFailure (show e)
                    Right bs2 -> do
                        bs1 <- LBS.readFile __FILE__
                        bs2 `shouldBe` bs1

fetchLazyByteStringCSpec :: Spec
fetchLazyByteStringCSpec =
    describe "fetchLazyByteStringC" $ do
        around withContentStore $
            it "raises CsErrorNoSuchObject when the object doesn't exist" $ \cs -> do
                let od = fromJust $ fromByteString (contentStoreDigest cs) invalidChecksum

                runCsConduit (yield od .| fetchLazyByteStringC cs .| sinkList) >>= (`shouldBe` Left (CsErrorNoSuchObject $ toHex od))

        around (withStoredFile __FILE__) $
            it "stored file and fetched file are the same" $ \(cs, digest) ->
                runCsConduit (yield digest .| fetchLazyByteStringC cs .| sinkList) >>= \case
                    Right [bs2] -> do
                        bs1 <- LBS.readFile __FILE__
                        bs2 `shouldBe` bs1
                    Right _ -> expectationFailure "fetchByteStringC returned more than one element"
                    Left e  -> expectationFailure (show e)

        around (withStoredFile __FILE__) $
            it "requesting two digests returns two files" $ \(cs, digest) ->
                runCsConduit (yieldMany [digest, digest] .| fetchLazyByteStringC cs .| sinkList) >>= \case
                    Left e    -> expectationFailure (show e)
                    Right lst -> length lst `shouldBe` 2

storeLazyByteStringSpec :: Spec
storeLazyByteStringSpec =
    describe "storeLazyByteString" $ do
        around withContentStore $
            it "storing a lazy bytestring should return a digest" $ \cs ->
                runCs (storeLazyByteString cs "The spice must flow.") >>= (`shouldSatisfy` anyDigest)

        around withContentStore $
            it "duplicate stores do not raise an error" $ \cs -> do
                first  <- runCs $ storeLazyByteString cs "blahblahblah"
                second <- runCs $ storeLazyByteString cs "blahblahblah"

                first `shouldSatisfy` anyDigest
                first `shouldBe` second

storeLazyByteStringCSpec :: Spec
storeLazyByteStringCSpec =
    describe "storeLazyByteStringC" $ do
        around withContentStore $
            it "storing a lazy bytestring should return a digest" $ \cs ->
                runCsConduit (yield "The spice must flow." .| storeLazyByteStringC cs .| sinkList) >>= \case
                    Left e    -> expectationFailure (show e)
                    Right lst -> length lst `shouldBe` 1

        around withContentStore $
            it "storing multiple lazy bytestrings should return multiple digests" $ \cs -> do
                let strs = ["I must not fear.",
                            "Fear is the mind-killer.",
                            "Fear is the little-death that brings total obliteration."]
                runCsConduit (yieldMany strs .| storeLazyByteStringC cs .| sinkList) >>= \case
                    Left e    -> expectationFailure (show e)
                    Right lst -> length lst `shouldBe` 3

storeLazyByteStringSinkSpec :: Spec
storeLazyByteStringSinkSpec =
    describe "storeLazyByteStringSink" $
        around withContentStore $
            it "storing multiple bytestrings should return a single digest" $ \cs -> do
                let strs = ["I must not fear.",
                            "Fear is the mind-killer.",
                            "Fear is the little-death that brings total obliteration."]
                runCsConduit (yieldMany strs .| storeLazyByteStringSink cs) >>= (`shouldSatisfy` anyDigest)

fetchFileSpec :: Spec
fetchFileSpec =
    describe "fetchFile" $ do
        around withContentStore $
            it "raises CsErrorNoSuchObject when the object doesn't exist" $ \cs -> do
                let od = fromJust $ fromByteString (contentStoreDigest cs) invalidChecksum
                    dest = "yyyyyyyyyyyyyyy"

                runCs (fetchFile cs od dest) >>= (`shouldBe` Left (CsErrorNoSuchObject $ toHex od))
                -- Verify the destination file was not created by conduit.  This can happen
                -- if sinkFile is used instead of sinkFileCautious.
                doesFileExist dest >>= (`shouldBe` False)

        around (withStoredFile __FILE__) $
            it "stored file and fetched file are the same" $ \(cs, digest) -> do
                (dest, h) <- openSystemTempFile "dest"
                hClose h

                -- Second, pull it back out of the store.
                runCs (fetchFile cs digest dest) >>= \case
                    Left e  -> removeFile dest >> expectationFailure (show e)
                    Right _ -> do
                        -- Third, compare the two files.  They should be the same.
                        bs1 <- BS.readFile __FILE__
                        bs2 <- BS.readFile dest
                        removeFile dest
                        bs2 `shouldBe` bs1

storeFileSpec :: Spec
storeFileSpec = do
    describe "storeFile" $
        around withContentStore $ do
            it "raises an IOException when trying to store a missing file" $ \cs ->
                runCs (storeFile cs "xxxxxxxxxxxxxxx") `shouldThrow` anyIOException

            it "storing an existing file should return a digest" $ \cs ->
                runCs (storeFile cs __FILE__) >>= (`shouldSatisfy` anyDigest)

    describe "storeFile duplicates" $
        around withContentStore $
            it "duplicate stores do not raise an error" $ \cs -> do
                first  <- runCs $ storeFile cs __FILE__
                second <- runCs $ storeFile cs __FILE__

                first `shouldSatisfy` anyDigest
                first `shouldBe` second

spec :: Spec
spec =
    describe "ContentStore" $ do
        contentStoreDigestSpec
        contentStoreValidSpec
        fetchByteStringSpec
        fetchByteStringCSpec
        fetchFileSpec
        fetchLazyByteStringSpec
        fetchLazyByteStringCSpec
        -- mkContentStoreSpec
        -- openContentStoreSpec
        storeByteStringSpec
        storeByteStringCSpec
        storeByteStringSinkSpec
        -- storeDirectorySpec
        storeFileSpec
        storeLazyByteStringSpec
        storeLazyByteStringCSpec
        storeLazyByteStringSinkSpec
