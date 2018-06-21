-- | Module: Data.ContentStore.Config
-- Copyright: (c) 2017 Red Hat, Inc.
-- License: LGPL
--
-- Maintainer: https://github.com/weldr
-- Stability: alpha
-- Portability: portable
--
-- Manage the content store's internal config file.

module Data.ContentStore.Config(Config(..),
                                defaultConfig,
                                readConfig,
                                writeConfig)
 where

-- Objects in the store are given file names that are their hash.
-- Lots of hash algorithms are available, which means we need to
-- know which one was used in a given store if we ever want to be
-- able to pull objects back out of it.  Thus, a config file.
--
-- Right now the only thing it's storing is the name of the hash
-- algorithm.  This may make using aeson seem like overkill, but
-- most of the config file parsing modules are unpleasant to use
-- and if this config file ever becomes too much more complicated,
-- we'll end up using aeson anyway.  Might as well start with it.

import           Data.Aeson
import           Data.Aeson.Types(Result(..), parseJSON)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import           Text.Toml(parseTomlDoc)

-- | Configuration information needed by the content store.
data Config = Config {
    -- | Is the data in the content store compressed?  If this option is missing
    -- in a config file, we assume that the data is not compressed.  This is to
    -- keep backwards compatibility with previous versions of the content store
    -- that did not support compression.  New content stores are created supporting
    -- compressed contents by default.
    confCompressed :: Bool,
    -- | What 'DigestAlgorithm' is in use by this content store?  While we do support
    -- several different algorithms, only one can ever be in use by a single content
    -- store.  Note that the 'ContentStore' record also stores this piece of
    -- information.  The difference is that here, we are only storing the text
    -- representation as given in a config file.
    confHash :: T.Text
 }

instance FromJSON Config where
    parseJSON = withObject "Config" $ \v ->
        Config <$> v .:? "compressed" .!= False
               <*> v .:? "hash" .!= "BLAKE2b256"

instance ToJSON Config where
    toJSON Config{..} = object [ "hash" .= toJSON confHash ]

-- | Construct a default 'Config' record useful for when creating a new 'ContentStore',
-- as with 'mkContentStore'.  Among other things, this is where the default hash
-- algorithm is defined.
defaultConfig :: Config
defaultConfig =
    Config { confCompressed = True,
             confHash = "BLAKE2b256" }

-- | Read a config file on disk, returning the 'Config' record on success and an error
-- message on error.  This function is typically not useful outside of content store
-- internals.
readConfig :: FilePath -> IO (Either T.Text Config)
readConfig path = do
    contents <- TIO.readFile path
    case parseTomlDoc "" contents of
        Left err  -> return $ Left $ T.pack $ show err
        Right tbl -> do let j = toJSON tbl
                        case (fromJSON j :: Result Config) of
                            Error err -> return $ Left $ T.pack $ show err
                            Success c -> return $ Right c

-- | Write a 'Config' object to disk.  This function is typically not useful outside
-- of content store internals.
writeConfig :: FilePath -> Config -> IO ()
writeConfig path Config{..} = TIO.writeFile path configText
 where
    configText = T.concat ["compressed = ", if confCompressed then "true" else "false", "\n",
                           "hash = \"", confHash, "\"\n"]
