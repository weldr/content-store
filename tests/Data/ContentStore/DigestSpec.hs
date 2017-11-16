{-# LANGUAGE OverloadedStrings #-}

module Data.ContentStore.DigestSpec(spec) where

import Test.Hspec
import Data.Maybe(fromJust)
import Data.ContentStore.Digest
import qualified Data.ByteString as B
import qualified Data.ByteArray as BA

testInput :: B.ByteString
testInput = "The Dude abides."

specSHA256 :: Spec
specSHA256 = do
    describe "Digest.DigestAlgorithm" $ do
        it "knows about SHA256" $
            digestName <$> getDigestAlgorithm "SHA256" `shouldBe` Just "SHA256"

        let daSHA256 = fromJust $ getDigestAlgorithm "SHA256"
        it "has the correct size for SHA256" $
            digestSize daSHA256 `shouldBe` 32

        let od = digestByteString daSHA256 testInput
        let hex = toHex od

        it "does SHA256 correctly" $
            BA.convert od `shouldBe` testDigest

        it "converts ObjectDigest to hex" $
            hex `shouldBe` testHex

    describe "Digest.DigestContext" $ do
        let da = fromJust $ getDigestAlgorithm "SHA256"
        let ctx = digestInit da
        it "can do digests piece by piece" $ do
            let hex = toHex $ digestFinalize $ digestUpdate ctx testInput
            hex `shouldBe` testHex

    describe "fromByteString" $ do
        let da = fromJust $ getDigestAlgorithm "SHA256"
        let od = digestByteString da testInput
        it "accepts a 32-byte ByteString" $
            fromByteString da testDigest `shouldBe` Just od
        it "rejects wrong-sized ByteString" $
            fromByteString da (B.take 20 testDigest) `shouldBe` Nothing
 where
    testDigest :: B.ByteString
    testDigest = "\xcd\x9c\xf3\x08\x62\x61\x24\xa7\x81\x81\xc0\x22\xfb\x9d\x0d\x51\x1a\xf1\xc1\x5d\x32\x20\x12\xf0\xa6\x9d\xfe\x86\x40\xc3\x40\xaa"

    testHex :: String
    testHex = "cd9cf308626124a78181c022fb9d0d511af1c15d322012f0a69dfe8640c340aa"

spec :: Spec
spec =
    describe "SHA256"
        specSHA256
