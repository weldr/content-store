{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ExistentialQuantification #-}

-- |
-- Module: Data.ContentStore.Digest
-- Copyright: (c) 2017 Red Hat, Inc.
-- License: LGPL
--
-- Maintainer: https://github.com/weldr
-- Stability: alpha
-- Portability: portable
--
-- Functions for working with digest algorithms as used by the content store

module Data.ContentStore.Digest(DigestAlgorithm,
                                getDigestAlgorithm,
                                digestName,
                                digestSize,
                                digestInit,
                                DigestContext,
                                digestUpdate,
                                digestUpdates,
                                digestFinalize,
                                digestByteString,
                                digestLazyByteString,
                                ObjectDigest,
                                toHex,
                                fromByteString)
 where

import           Crypto.Hash
import           Data.ByteArray(Bytes, ByteArrayAccess, convert)
import           Data.ByteArray.Encoding(Base(..), convertToBase)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import           Data.Text.Encoding(decodeUtf8)

-- cryptonite is about 50% too clever with its types, if you ask me.
-- Here's simpler ones that we can use in ContentStore.

--
-- DIGEST TYPES
--

-- | A DigestAlgorithm represents one specific hash algorithm.
data DigestAlgorithm = forall a. (HashAlgorithm a, Show a) => DigestAlgorithm a

-- | Holds the context for a given instance of a 'DigestAlgorithm'.
data DigestContext = forall a. HashAlgorithm a => DigestContext (Context a)

-- | ObjectDigest is the (binary) representation of a digest.
newtype ObjectDigest = ObjectDigest Bytes
    deriving (Eq, Ord, ByteArrayAccess)

instance Show ObjectDigest where
    show (ObjectDigest b) = show b

--
-- PRIVATE FUNCTIONS
--

fromDigest :: Digest a -> ObjectDigest
fromDigest = ObjectDigest . convert

hashLazyWith :: HashAlgorithm a => a -> LBS.ByteString -> Digest a
hashLazyWith _ = hashlazy

--
-- PUBLIC FUNCTIONS
--

-- | Get the name of this 'DigestAlgorithm'.
digestName :: DigestAlgorithm -> String
digestName (DigestAlgorithm a) = show a

-- | The size of the 'ObjectDigest' returned by this 'DigestAlgorithm'.
digestSize :: DigestAlgorithm -> Int
digestSize (DigestAlgorithm a) = hashDigestSize a

-- | Initialize a new 'DigestContext' for this 'DigestAlgorithm'.
digestInit :: DigestAlgorithm -> DigestContext
digestInit (DigestAlgorithm a) = DigestContext $ hashInitWith a

-- | Update the 'DigestContext' with one 'ByteString'/'Bytes'/etc item.
digestUpdate :: ByteArrayAccess ba => DigestContext -> ba -> DigestContext
digestUpdate (DigestContext ctx) = DigestContext . hashUpdate ctx

-- | Update the 'DigestContext' with many 'ByteString'/'Bytes'/etc. items.
digestUpdates :: ByteArrayAccess ba => DigestContext -> [ba] -> DigestContext
digestUpdates (DigestContext ctx) = DigestContext . hashUpdates ctx

-- | Finish the digest, returning an 'ObjectDigest'.
digestFinalize :: DigestContext -> ObjectDigest
digestFinalize (DigestContext ctx) = fromDigest $ hashFinalize ctx

-- | Hash a strict 'ByteString' into an 'ObjectDigest'.
digestByteString :: DigestAlgorithm -> BS.ByteString -> ObjectDigest
digestByteString (DigestAlgorithm a) = fromDigest . hashWith a

-- | Hash a lazy 'Data.ByteString.Lazy.ByteString' into an 'ObjectDigest'.
digestLazyByteString :: DigestAlgorithm -> LBS.ByteString -> ObjectDigest
digestLazyByteString (DigestAlgorithm a) = fromDigest . hashLazyWith a

-- | Check and convert a 'ByteString' into an 'ObjectDigest'.
fromByteString :: ByteArrayAccess ba => DigestAlgorithm -> ba -> Maybe ObjectDigest
fromByteString (DigestAlgorithm a) = fmap fromDigest . digestFromByteStringWith a
-- helper for above
digestFromByteStringWith :: (HashAlgorithm a, ByteArrayAccess ba) => a -> ba -> Maybe (Digest a)
digestFromByteStringWith _ = digestFromByteString

-- | Convert an 'ObjectDigest' to its hex representation.
-- TODO: probably more efficient if we can just coerce the converted Bytes..
toHex :: ObjectDigest -> String
toHex = T.unpack . decodeUtf8 . convertToBase Base16


-- | Given the 'Text' name of a digest algorithm, return a 'DigestAlgorithm'
-- (or Nothing if we don't recognize the DigestAlgorithm's name).
getDigestAlgorithm :: T.Text -> Maybe DigestAlgorithm
getDigestAlgorithm "SHA256"     = Just $ DigestAlgorithm SHA256
getDigestAlgorithm "SHA512"     = Just $ DigestAlgorithm SHA512
getDigestAlgorithm "BLAKE2"     = Just $ DigestAlgorithm Blake2b_512
getDigestAlgorithm "BLAKE2b"    = Just $ DigestAlgorithm Blake2b_512
getDigestAlgorithm "BLAKE2b512" = Just $ DigestAlgorithm Blake2b_512
getDigestAlgorithm "BLAKE2b256" = Just $ DigestAlgorithm Blake2b_256
getDigestAlgorithm "BLAKE2s"    = Just $ DigestAlgorithm Blake2s_256
getDigestAlgorithm "BLAKE2s256" = Just $ DigestAlgorithm Blake2s_256
getDigestAlgorithm _            = Nothing
