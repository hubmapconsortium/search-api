#!/usr/bin/env bash
set -o errexit

. test-utils.sh

cd `dirname $0`

topdir=${PWD}/../..
reldir=`realpath --relative-to $topdir .` \
  || die 'On Mac? "brew install coreutils" to get realpath.'

start libs/assay_type
CMD="python ./assay_type.py"
eval $CMD || die "The doctest entries in ${reldir}/assay_type.py need to be updated"
end libs/assay_type
