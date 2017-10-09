module Data.ContentStoreSpec(spec) where

import Test.Hspec
import Data.ContentStore

spec :: Spec
spec =
    describe "ContentStore" $
        it "has dummy test" $
            () `shouldBe` ()
