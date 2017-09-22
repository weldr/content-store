{-# LANGUAGE OverloadedStrings #-}

module Data.ContentStore.Digest(ObjectDigest(..),
                                hashByteString,
                                hashLazyByteString)
 where

import           Crypto.Hash(Digest, hash, hashlazy)
import           Crypto.Hash.Algorithms(SHA256, SHA512)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T

-- Only a subset of the algorithms provided by cryptonite are supported
-- by our content store.  You can't have everything.  Because each
-- algorithm is implemented as a different type (though, all are
-- members of the HashAlgorithm typeclass), we need to wrap them up into
-- a single data type if we are to write generic hashing functions.  And
-- we want to do that to prevent the spread of knowing which algorithm
-- is in use throughout the code.
data ObjectDigest = ObjectSHA256 (Digest SHA256)
                  | ObjectSHA512 (Digest SHA512)

hashByteString :: T.Text -> BS.ByteString -> Maybe ObjectDigest
hashByteString algo bs = case algo of
    "SHA256" -> Just $ ObjectSHA256 (hash bs :: Digest SHA256)
    "SHA512" -> Just $ ObjectSHA512 (hash bs :: Digest SHA512)
    _        -> Nothing

hashLazyByteString :: T.Text -> LBS.ByteString -> Maybe ObjectDigest
hashLazyByteString algo lbs = case algo of
    "SHA256" -> Just $ ObjectSHA256 (hashlazy lbs :: Digest SHA256)
    "SHA512" -> Just $ ObjectSHA512 (hashlazy lbs :: Digest SHA512)
    _        -> Nothing
