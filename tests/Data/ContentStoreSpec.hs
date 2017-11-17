{-# LANGUAGE LambdaCase #-}

module Data.ContentStoreSpec(spec)
 where

import Control.Monad.Except(runExceptT)
import System.Directory(createDirectory)
import System.FilePath.Posix((</>))
import System.IO.Temp(withSystemTempDirectory)
import Test.Hspec

import Data.ContentStore
import Data.ContentStore.Digest

{-# ANN module ("HLint: ignore Eta reduce" :: String) #-}
{-# ANN module ("HLint: ignore Reduce duplication" :: String) #-}

withContentStore :: ActionWith ContentStore -> IO ()
withContentStore action = withTempDir $ \d ->
    runExceptT (mkContentStore d) >>= \case
        Left e     -> expectationFailure (show e)
        Right repo -> action repo

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

spec :: Spec
spec =
    describe "ContentStore" $ do
        contentStoreValidSpec
        contentStoreDigestSpec
