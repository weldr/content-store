{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module Data.ContentStoreSpec(spec)
 where

import           Control.Monad.Except(ExceptT, runExceptT)
import           Control.Monad.Trans.Resource(ResourceT, runResourceT)
import qualified Data.ByteString as BS
import           Data.Either(isRight)
import           Data.Maybe(fromJust)
import           System.Directory(createDirectory, doesFileExist)
import           System.FilePath.Posix((</>))
import           System.IO.Temp(withSystemTempDirectory)
import           Test.Hspec

import Data.ContentStore
import Data.ContentStore.Digest

{-# ANN module ("HLint: ignore Eta reduce" :: String) #-}
{-# ANN module ("HLint: ignore Reduce duplication" :: String) #-}

anyDigest :: Either CsError ObjectDigest -> Bool
anyDigest = isRight

runCs :: ResourceT (ExceptT CsError IO) a -> IO (Either CsError a)
runCs = runExceptT . runResourceT

withContentStore :: ActionWith ContentStore -> IO ()
withContentStore action = withTempDir $ \d ->
    runExceptT (mkContentStore d) >>= either (expectationFailure . show) action

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

fetchFileSpec :: Spec
fetchFileSpec =
    describe "fetchFile" $
        around withContentStore $
            it "raises CsErrorNoSuchObject when the object doesn't exist" $ \cs -> do
                let cksum = "\x82\xe4\x69\x8a\x1e\xbe\x82\xed\xae\xc8\xe8\xa6\xdc\xf1\x8d\x57\xbb\x5a\xc6\x71\x8b\x6b\xf2\xbb\xc3\x8a\x00\xda\x2e\x29\x34\xe6" :: BS.ByteString
                    od = fromJust $ fromByteString (contentStoreDigest cs) cksum
                    dest = "yyyyyyyyyyyyyyy"

                runCs (fetchFile cs od dest) >>= (`shouldBe` Left (CsErrorNoSuchObject $ toHex od))
                -- Verify the destination file was not created by conduit.  This can happen
                -- if sinkFile is used instead of sinkFileCautious.
                doesFileExist dest >>= (`shouldBe` False)

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
        contentStoreValidSpec
        contentStoreDigestSpec
        fetchFileSpec
        storeFileSpec
