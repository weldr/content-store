sandbox:
	[ -d .cabal-sandbox ] || cabal sandbox init && cabal update

hlint: sandbox
	hlint .

tests: sandbox
	cabal install --dependencies-only --enable-tests --force-reinstalls
	cabal configure --enable-tests --enable-coverage --ghc-option=-DTEST
	cabal build
	cabal test --show-details=always

ci: tests hlint

ci_after_success:
	hpc-coveralls --display-report spec
