#!/usr/bin/env bash
set -o errexit

red=`tput setaf 1`
green=`tput setaf 2`
reset=`tput sgr0`
start() { [[ -z $CI ]] || echo travis_fold':'start:$1; echo $green$1$reset; }
end() { [[ -z $CI ]] || echo travis_fold':'end:$1; }
die() { set +v; echo "$red$*$reset" 1>&2 ; exit 1; }

# TODO: These are being run inside schema/,
# but it should be done at the top level.

# start flake8
# flake8 || die 'Try: autopep8 --in-place --aggressive -r .'
# end flake8
#
# start doctests
# find src | grep '\.py$' | xargs python -m doctest
# end doctests

cd schema
./test.sh
cd -

start changelog
if [ "$TRAVIS_BRANCH" != 'master' ]; then
  diff CHANGELOG.md <(curl -s https://raw.githubusercontent.com/hubmapconsortium/search-api/master/CHANGELOG.md) \
    && die 'Update CHANGELOG.md'
fi
end changelog
