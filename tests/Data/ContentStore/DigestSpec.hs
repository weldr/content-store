module Data.ContentStore.DigestSpec(spec) where

import Test.Hspec
import Data.Maybe(fromJust)
import Data.ContentStore.Digest
import qualified Data.ByteString as B
import qualified Data.ByteArray as BA

{-# ANN module ("HLint: ignore Reduce duplication" :: String) #-}

testInput :: B.ByteString
testInput = "The Dude abides."

specBLAKE2 :: Spec
specBLAKE2 =
    describe "Digest.DigestAlgorithm" $ do
        it "knows about BLAKE2" $
            digestName <$> getDigestAlgorithm "BLAKE2" `shouldBe` Just "Blake2b_512"

        let daBLAKE2 = fromJust $ getDigestAlgorithm "BLAKE2"
        it "has the correct size for BLAKE2" $
            digestSize daBLAKE2 `shouldBe` 64

        let od = digestByteString daBLAKE2 testInput
        let hex = toHex od

        it "does BLAKE2 correctly" $
            BA.convert od `shouldBe` testDigest

        it "converts ObjectDigest to hex" $
            hex `shouldBe` testHex
 where
    testDigest :: B.ByteString
    testDigest = "\x0e\x5e\xf4\x9c\x6b\x5d\xf5\x6f\xca\x89\x9f\xef\xde\x64\x8e\xc6\xc5\x42\x95\xe4\x40\x68\x9d\xe9\xd0\x2c\xaf\xc4\x87\xa6\xdb\xac\x7c\x79\x60\x1e\x2d\xa7\xb3\x13\xc8\x52\xa7\xba\xdd\x1d\x31\x97\x22\xa9\x6b\xd0\xd9\x1c\x06\x75\x97\x4b\x07\x69\xbe\x12\x45\x94"

    testHex :: String
    testHex = "0e5ef49c6b5df56fca899fefde648ec6c54295e440689de9d02cafc487a6dbac7c79601e2da7b313c852a7badd1d319722a96bd0d91c0675974b0769be124594"

specBLAKE2b :: Spec
specBLAKE2b =
    describe "Digest.DigestAlgorithm" $ do
        it "knows about BLAKE2b" $
            digestName <$> getDigestAlgorithm "BLAKE2b" `shouldBe` Just "Blake2b_512"

        let daBLAKE2b = fromJust $ getDigestAlgorithm "BLAKE2b"
        it "has the correct size for BLAKE2b" $
            digestSize daBLAKE2b `shouldBe` 64

        let od = digestByteString daBLAKE2b testInput
        let hex = toHex od

        it "does BLAKE2b correctly" $
            BA.convert od `shouldBe` testDigest

        it "converts ObjectDigest to hex" $
            hex `shouldBe` testHex
 where
    testDigest :: B.ByteString
    testDigest = "\x0e\x5e\xf4\x9c\x6b\x5d\xf5\x6f\xca\x89\x9f\xef\xde\x64\x8e\xc6\xc5\x42\x95\xe4\x40\x68\x9d\xe9\xd0\x2c\xaf\xc4\x87\xa6\xdb\xac\x7c\x79\x60\x1e\x2d\xa7\xb3\x13\xc8\x52\xa7\xba\xdd\x1d\x31\x97\x22\xa9\x6b\xd0\xd9\x1c\x06\x75\x97\x4b\x07\x69\xbe\x12\x45\x94"

    testHex :: String
    testHex = "0e5ef49c6b5df56fca899fefde648ec6c54295e440689de9d02cafc487a6dbac7c79601e2da7b313c852a7badd1d319722a96bd0d91c0675974b0769be124594"

specBLAKE2b512 :: Spec
specBLAKE2b512 =
    describe "Digest.DigestAlgorithm" $ do
        it "knows about BLAKE2b512" $
            digestName <$> getDigestAlgorithm "BLAKE2b512" `shouldBe` Just "Blake2b_512"

        let daBLAKE2b512 = fromJust $ getDigestAlgorithm "BLAKE2b512"
        it "has the correct size for BLAKE2b512" $
            digestSize daBLAKE2b512 `shouldBe` 64

        let od = digestByteString daBLAKE2b512 testInput
        let hex = toHex od

        it "does BLAKE2b512 correctly" $
            BA.convert od `shouldBe` testDigest

        it "converts ObjectDigest to hex" $
            hex `shouldBe` testHex
 where
    testDigest :: B.ByteString
    testDigest = "\x0e\x5e\xf4\x9c\x6b\x5d\xf5\x6f\xca\x89\x9f\xef\xde\x64\x8e\xc6\xc5\x42\x95\xe4\x40\x68\x9d\xe9\xd0\x2c\xaf\xc4\x87\xa6\xdb\xac\x7c\x79\x60\x1e\x2d\xa7\xb3\x13\xc8\x52\xa7\xba\xdd\x1d\x31\x97\x22\xa9\x6b\xd0\xd9\x1c\x06\x75\x97\x4b\x07\x69\xbe\x12\x45\x94"

    testHex :: String
    testHex = "0e5ef49c6b5df56fca899fefde648ec6c54295e440689de9d02cafc487a6dbac7c79601e2da7b313c852a7badd1d319722a96bd0d91c0675974b0769be124594"

specBLAKE2b256 :: Spec
specBLAKE2b256 =
    describe "Digest.DigestAlgorithm" $ do
        it "knows about BLAKE2b256" $
            digestName <$> getDigestAlgorithm "BLAKE2b256" `shouldBe` Just "Blake2b_256"

        let daBLAKE2b256 = fromJust $ getDigestAlgorithm "BLAKE2b256"
        it "has the correct size for BLAKE2b256" $
            digestSize daBLAKE2b256 `shouldBe` 32

        let od = digestByteString daBLAKE2b256 testInput
        let hex = toHex od

        it "does BLAKE2b256 correctly" $
            BA.convert od `shouldBe` testDigest

        it "converts ObjectDigest to hex" $
            hex `shouldBe` testHex
 where
    testDigest :: B.ByteString
    testDigest = "\x82\xe4\x69\x8a\x1e\xbe\x82\xed\xae\xc8\xe8\xa6\xdc\xf1\x8d\x57\xbb\x5a\xc6\x71\x8b\x6b\xf2\xbb\xc3\x8a\x00\xda\x2e\x29\x34\xe6"

    testHex :: String
    testHex = "82e4698a1ebe82edaec8e8a6dcf18d57bb5ac6718b6bf2bbc38a00da2e2934e6"

specBLAKE2s :: Spec
specBLAKE2s =
    describe "Digest.DigestAlgorithm" $ do
        it "knows about BLAKE2s" $
            digestName <$> getDigestAlgorithm "BLAKE2s" `shouldBe` Just "Blake2s_256"

        let daBLAKE2s = fromJust $ getDigestAlgorithm "BLAKE2s"
        it "has the correct size for BLAKE2s" $
            digestSize daBLAKE2s `shouldBe` 32

        let od = digestByteString daBLAKE2s testInput
        let hex = toHex od

        it "does BLAKE2s correctly" $
            BA.convert od `shouldBe` testDigest

        it "converts ObjectDigest to hex" $
            hex `shouldBe` testHex
 where
    testDigest :: B.ByteString
    testDigest = "\xfe\x04\xd7\x14\x3e\x23\x1a\x36\xc7\xbd\x1f\x9b\xdb\x36\x04\x56\xdd\xe8\x1c\xb9\x22\x3a\xab\x25\x5f\x74\x9f\x80\xe9\x60\xd7\x9f"

    testHex :: String
    testHex = "fe04d7143e231a36c7bd1f9bdb360456dde81cb9223aab255f749f80e960d79f"

specBLAKE2s256 :: Spec
specBLAKE2s256 =
    describe "Digest.DigestAlgorithm" $ do
        it "knows about BLAKE2s256" $
            digestName <$> getDigestAlgorithm "BLAKE2s256" `shouldBe` Just "Blake2s_256"

        let daBLAKE2s256 = fromJust $ getDigestAlgorithm "BLAKE2s256"
        it "has the correct size for BLAKE2s256" $
            digestSize daBLAKE2s256 `shouldBe` 32

        let od = digestByteString daBLAKE2s256 testInput
        let hex = toHex od

        it "does BLAKE2s256 correctly" $
            BA.convert od `shouldBe` testDigest

        it "converts ObjectDigest to hex" $
            hex `shouldBe` testHex
 where
    testDigest :: B.ByteString
    testDigest = "\xfe\x04\xd7\x14\x3e\x23\x1a\x36\xc7\xbd\x1f\x9b\xdb\x36\x04\x56\xdd\xe8\x1c\xb9\x22\x3a\xab\x25\x5f\x74\x9f\x80\xe9\x60\xd7\x9f"

    testHex :: String
    testHex = "fe04d7143e231a36c7bd1f9bdb360456dde81cb9223aab255f749f80e960d79f"

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

    describe "BLAKE2"
        specBLAKE2

    describe "BLAKE2b"
        specBLAKE2b

    describe "BLAKE2b512"
        specBLAKE2b512

    describe "BLAKE2b256"
        specBLAKE2b256

    describe "BLAKE2s"
        specBLAKE2s

    describe "BLAKE2s256"
        specBLAKE2s256
