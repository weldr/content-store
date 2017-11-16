{-# LANGUAGE OverloadedStrings #-}

module Data.ContentStore.DigestSpec(spec) where

import Test.Hspec
import Data.Maybe(fromJust)
import Data.ContentStore.Digest
import qualified Data.ByteString as B
import qualified Data.ByteArray as BA

{-# ANN module ("HLint: ignore Reduce duplication" :: String) #-}

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

specSHA512 :: Spec
specSHA512 = do
    describe "Digest.DigestAlgorithm" $ do
        it "knows about SHA512" $
            digestName <$> getDigestAlgorithm "SHA512" `shouldBe` Just "SHA512"

        let daSHA512 = fromJust $ getDigestAlgorithm "SHA512"
        it "has the correct size for SHA512" $
            digestSize daSHA512 `shouldBe` 64

        let od = digestByteString daSHA512 testInput
        let hex = toHex od

        it "does SHA512 correctly" $
            BA.convert od `shouldBe` testDigest

        it "converts ObjectDigest to hex" $
            hex `shouldBe` testHex

    describe "Digest.DigestContext" $ do
        let da = fromJust $ getDigestAlgorithm "SHA512"
        let ctx = digestInit da
        it "can do digests piece by piece" $ do
            let hex = toHex $ digestFinalize $ digestUpdate ctx testInput
            hex `shouldBe` testHex

    describe "fromByteString" $ do
        let da = fromJust $ getDigestAlgorithm "SHA512"
        let od = digestByteString da testInput
        it "accepts a 32-byte ByteString" $
            fromByteString da testDigest `shouldBe` Just od
        it "rejects wrong-sized ByteString" $
            fromByteString da (B.take 20 testDigest) `shouldBe` Nothing
 where
    testDigest :: B.ByteString
    testDigest = "\x05\x07\xdb\x94\x58\xc9\xfe\xce\x37\xd5\xfb\x54\xc4\xda\xf4\xe8\x21\x14\xda\xb1\x53\xdc\x0b\x0e\x15\x74\x49\x77\x54\x94\x35\xdc\x78\xc7\x0c\xef\x51\x3e\xb7\x3a\x4f\x80\x81\x2a\x6e\xd5\x73\x1e\xcf\xff\xb4\x95\x98\x56\xfa\x94\x40\x30\xec\x20\x78\x45\xae\x34"

    testHex :: String
    testHex = "0507db9458c9fece37d5fb54c4daf4e82114dab153dc0b0e15744977549435dc78c70cef513eb73a4f80812a6ed5731ecfffb4959856fa944030ec207845ae34"

spec :: Spec
spec = do
    describe "SHA256"
        specSHA256

    describe "SHA512"
        specSHA512
