#!/usr/bin/env bash
set -o errexit

. test-utils.sh

start version
if [ "$TRAVIS_BRANCH" != 'master' ] && [ "$TRAVIS_BRANCH" != 'devel' ] ; then
diff src/VERSION <(curl -s https://raw.githubusercontent.com/hubmapconsortium/search-api/devel/src/VERSION) \
  && die 'Bump VERSION'
fi
end version

cd `dirname $0`

start portal/flake8
flake8 \
  || die "Try: autopep8 --in-place --aggressive -r $PWD"
end portal/flake8

start portal/doctests
cd ../../..
for F in elasticsearch/addl_index_transformations/portal/*.py; do
  python -m doctest -o REPORT_NDIFF $F
done
cd -
end portal/doctests
