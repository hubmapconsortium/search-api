#!/usr/bin/env bash
set -o errexit

start() { echo travis_fold':'start:$1; echo $1; }
end() { echo travis_fold':'end:$1; }
die() { set +v; echo "$*" 1>&2 ; sleep 1; exit 1; }

start flake8
flake8 \
  || die "Try: autopep8 --in-place --aggressive -r ."
end flake8

start doctests
ls *.py | xargs python -m doctest
end doctests
