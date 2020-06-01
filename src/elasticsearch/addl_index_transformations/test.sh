#!/usr/bin/env bash
set -o errexit

start() { echo travis_fold':'start:$1; echo $1; }
end() { echo travis_fold':'end:$1; }
die() { set +v; echo "$*" 1>&2 ; sleep 1; exit 1; }

cd `dirname $0`

start requirements
# After brew install, pipenv did not work for me locally,
# and getting it to work on Travis would be another chore,
# so for now, just confirm that dependencies here
# are mirrored there.
while read LINE
do
    grep "$LINE" ../../../Pipfile || die "Add '$LINE' to Pipfile."
done < <(cat requirements*.txt | sed 's/==/ = /')
end requirements

start flake8
flake8 \
  || die "Try: autopep8 --in-place --aggressive -r ."
end flake8

start doctests
ls *.py | xargs python -m doctest
end doctests
