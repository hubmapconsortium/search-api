#!/bin/bash
################################################################################
# Script to do a "full" reindex of Neo4j data into ElasticSearch using the
# strategy specified in the fresh_indices.ini file.
################################################################################

# Choose to exit when unbound variables referenced
set -u
# Might choose to stop on an error (vs continuing or trapping) in the
# future, but for now use return status from grep with test, and do not
# exit just because something isn't found.
# set -e

################################################################################
# Help function
################################################################################
Help()
{
   # Display Help
   echo ""
   echo "****************************************"
   echo "HELP: $0"
   echo
   echo "Script to initialize a fresh ElasticSearch index using Neo4j data."
   echo
   echo "A new ElasticSearch index is created using the strategy specified in the fresh_index.ini file."
   echo "The new index is created and mostly filled without taking the search-api offline. During"
   echo "this time, the new index has a temporary name, and the existing index continues supporting"
   echo "Production."
   echo
   echo "Before filling the new index, timestamps are captured from the most recently modified"
   echo "documents in the ElasticSearch index with every document. This index is configured in"
   echo "search-config.yaml as 'entities'->default_index->'private' i.e. typically hm_*_consortium_entities."
   echo
   echo "After the new index is filled, the search-api service must be taken offline.  Then, any documents"
   echo "modified after the captured timestamps are 're-indexed' in the new index. After those documents"
   echo "are refreshed to reflect activity that happened after the timestamps were captured, the existing"
   echo "index is renamed and can be manually deleted.  The new index is renamed to support Production."
   echo
   echo "When the index renaming activity is compete, this script will wait for the ElasticSearch index health to"
   echo "become green.  After that, the search-api can be returned to service, and will use the fresh index."
   echo
   echo "Syntax: $0 [-option] [command]"
   echo "[command]"
   echo "create - Create a new ElasticSearch index with a temporary name, filled with documents indexed from Neo4j data."
   echo "catch-up - While search-api is down"
   echo "            * re-index documents which were modified after the create started,"
   echo "            * rename the old index so it can be deleted,"
   echo "            * rename the new index for use by Production,"
   echo "            * and wait for the new index to have green health."
   echo "go-live - Swap indices around so the results of 'create' and 'catch-up' commands becomes the indices used by the Search API."
   echo "            * Names of current and new indices are taken from the newest exec_info/op_data*.json file."
   echo "            * The current indices will be renamed with a 'flush' prefix."
   echo "            * The new indices will taken on the name expected by Search API."
   echo "            * The script will wait for 'green health' on each renamed index."
   echo
   echo "[-option]"
   echo "-h Display this help"
   echo "-v Verbose output"
}

################################################################################
# Verify the needs of this script are available, the version is acceptable, etc.
################################################################################
StartupVerifications()
{
    # No version requirement for Python 3, but don't expect it to report
    # a version if it is unavailable
    if ! python3 --version | grep '^Python 3.[0-9]' > /dev/null; then
	    bail_out_errors+=("Python 3 does not seem to be available")
    elif [[ "$arg_verbose" == true ]]; then
	    echo Python 3 found - `python3 --version`
    fi

    if [[ ! -f "./token_holder" ]]; then
	bail_out_errors+=("The file 'token_holder' is not found in `pwd`")
    fi
}

################################################################################
# Set variables used by this script, including defaults which can be  overridden
# by command line arguments.
################################################################################
# Date stamp to append to files which this script creates
printf -v date_stamp '%(%Y-%m-%d)T' -1

# Commands accepted in the script arguments after the options, as described in Help()
recognized_commands=("create","catch-up","go-live")

# Pull the names of the destination indices from the same YAML which will be
# used for reindexing.
readarray -t entities_portal_indices < <(
python -c   'import yaml,sys; \
            y=yaml.safe_load(sys.stdin); \
            print(y["indices"]["entities"]["public"]); \
            print(y["indices"]["entities"]["private"]); \
            print(y["indices"]["portal"]["public"]); \
            print(y["indices"]["portal"]["private"])' < ../../src/instance/search-config.yaml
)

################################################################################
# Set internal variables used by this script
################################################################################
# Array of accumulated errors causing script to give up instead of
# generating a CSV.
bail_out_errors=()
# Flag tracking script option for verbose output.
arg_verbose=false
# String tracking script option for severity levels.
arg_output_dir='./exec_info' # KBKBKB @TODO get from fresh_indices.ini
# Flag forcing a smaller set of data to be used while creating and catching-up.
use_dev_subset=true
# Exit codes for script outcome
EXIT_SUCCESS=0
EXIT_VERIFICATION_FAILURE=254
EXIT_INVALID_OPTION=255

################################################################################
# Get and validate options supplied on the command line.
################################################################################
while getopts ":hv" option; do
    case $option in
	h) # display Help
	    Help
	    exit $EXIT_SUCCESS;;
	v) # verbose
	    arg_verbose=true
	    if [[ "$arg_verbose" == true ]]; then echo "Verbose output enabled"; fi ;;
	\?) # Invalid option
	    echo "Error: Invalid option"
	    exit $EXIT_INVALID_OPTION;;
    esac
done

# Expect only on argument after options are processed, for a recognized command.
shift $((OPTIND - 1))

if [ $# -ne 1 ]; then
    bail_out_errors+=("One and only one command is accepted.  See $0 -h")
else
  cmd=$@
  case "${recognized_commands[*]}" in (*${cmd}*)
      echo "Command to execute: '$cmd'" ;;
    (*)
      bail_out_errors+=("Unrecognized command '$cmd'. See $0 -h") ;;
  esac
fi

# Verify resources this script needs are available.
StartupVerifications

# Verify the specified output directory is writeable.
if [ ! -w $arg_output_dir ]; then
    bail_out_errors+=("Unable to write files to '${arg_output_dir}'.")
fi

# If any errors have accumulated which would prevent scanning, print
# them to stderr and exit
if (( ${#bail_out_errors[@]} != 0 )); then
    echo Identified ${#bail_out_errors[@]} errors. >&2
    for error in "${bail_out_errors[@]}"; do
      echo " $error" >&2
    done
    echo -e 'Exiting.\a' >&2
    exit $EXIT_VERIFICATION_FAILURE
fi

# Display settings, either from command line arguments or defaults.
echo "Output will be in ${arg_output_dir}"
echo

if [[ "$cmd" == "create" ]]; then
  echo "Creating new indices to replace these configured in app.cfg:"
  for index in ${entities_portal_indices[@]%,}; do
    echo -e "\t$index "
  done
elif [[ "$cmd" == "catch-up" ]]; then
  echo "Using op_data from the most current $arg_output_dir/op_data*.json file to re-index any entities touch since the 'create' command."
elif [[ "$cmd" == "go-live" ]]; then
  echo "Using op_data from the most current $arg_output_dir/op_data*.json file, swapping index names so Search API can use new indices, "
else
  echo "Unexpectedly tried to execute with cmd='$cmd'"
fi
MYPYPATH=../../src:../../src/search-adaptor/src:../../src/search-adaptor/src/libs:../../src/search-adaptor/src/translator
PYTHONPATH=$MYPYPATH python3 fresh_indices.py $cmd `cat ./token_holder`

exit $EXIT_SUCCESS
