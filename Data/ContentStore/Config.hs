{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

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

module Data.ContentStore.Config(Config(..),
                                defaultConfig,
                                readConfig,
                                writeConfig)
 where

import           Data.Aeson
import           Data.Aeson.Types(Result(..), parseJSON)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import           Text.Toml(parseTomlDoc)

{-# ANN module ("HLint: ignore Use newtype instead of data" :: String) #-}

data Config = Config {
    confHash :: T.Text
 }

instance FromJSON Config where
    parseJSON = withObject "Config" $ \v ->
        Config <$> v .:? "hash" .!= "BLAKE2b256"

instance ToJSON Config where
    toJSON Config{..} = object [ "hash" .= toJSON confHash ]

defaultConfig :: Config
defaultConfig =
    Config { confHash = "BLAKE2b256" }

readConfig :: FilePath -> IO (Either T.Text Config)
readConfig path = do
    contents <- TIO.readFile path
    case parseTomlDoc "" contents of
        Left err  -> return $ Left $ T.pack $ show err
        Right tbl -> do let j = toJSON tbl
                        case (fromJSON j :: Result Config) of
                            Error err -> return $ Left $ T.pack $ show err
                            Success c -> return $ Right c

writeConfig :: FilePath -> Config -> IO ()
writeConfig path Config{..} = TIO.writeFile path configText
 where
    configText = T.concat ["hash = \"", confHash, "\"\n"]
