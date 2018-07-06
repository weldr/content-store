## ingore this

## 0.2.1

* Allow newer versions of aeson, conduit, conduit-combinators, conduit-extra,
  and lzma-conduit.

## 0.2.0

* Added lots of test cases.
* Add support for compressed content stores.  This requires lzma-conduit.
* Fix a file creation bug in fetchFile.  If you attempted to fetch an object
  that did not exist, the output file would still be created.  This no longer
  happens.
* Fix fetchByteString and fetchLazyByteString to return the entire object,
  not just the first chunk of it.  This was caused by improper use of headC.
* Fix a bug in fetchByteStringC and fetchLazyByteStringC that was causing
  these functions to output objects as multiple ByteStrings.
* Remove duplicate MonadIO constraints.  These were being duplicated by the
  use of MonadResource constraints.

## 0.1.1

* Relax overly strict aeson, monad-control, temporary, and text requirements.

## 0.1.0

* Initial release.
