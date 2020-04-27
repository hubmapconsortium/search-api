#!/usr/bin/env bash
set -o errexit

red=`tput setaf 1`
green=`tput setaf 2`
reset=`tput sgr0`
start() { [[ -z $CI ]] || echo travis_fold':'start:$1; echo $green$1$reset; }
end() { [[ -z $CI ]] || echo travis_fold':'end:$1; }
die() { set +v; echo "$red$*$reset" 1>&2 ; exit 1; }

start flake8
flake8 || die 'Try: autopep8 --in-place --aggressive -r .'
end flake8

start doctests
find src | grep '\.py$' | xargs python -m doctest
end doctests

start definitions
CMD='src/tsv-to-yaml.py --definitions defininitions'
$CMD > definitions.test.yaml
diff --ignore-blank-lines definitions{,.test}.yaml \
  || die "To refresh: $CMD > definitions.yaml"
rm definitions.test.yaml
end definitions
