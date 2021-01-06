#!/usr/bin/env bash
set -o errexit

. test-utils.sh

cd `dirname $0`

topdir=${PWD}/../..
reldir=`realpath --relative-to $topdir .` \
  || die 'On Mac? "brew install coreutils" to get realpath.'

start libs/assay_type
CMD="python ./assay_type.py"
diff ${topdir}/examples/libs/assay_type_out.txt <( eval $CMD ) \
  || die "Try: python ${reldir}/assay_type.py > examples/libs/assay_type_out.txt"
end libs/assay_type

