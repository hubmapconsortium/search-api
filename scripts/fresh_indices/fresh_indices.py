import glob
import json
import os
import re
import sys
import configparser
import logging
import importlib

from pathlib import Path

from yaml import safe_load
from datetime import datetime, timedelta

# Import search-api and search-adaptor resources
from es_writer import ESWriter
from indexer import Indexer
from opensearch_helper_functions import *
from hubmap_translator import *

# Import search-api/scripts resources
from FillStrategyType import FillStrategyType
from AggQueryType import AggQueryType
from es_manager import ESManager

def init():
    global logger
    global fill_strategy
    global appcfg
    global EXEC_INFO_DIR

    #
    # Read configuration from the INI file and set global constants
    #
    try:
        Config = configparser.ConfigParser()
        config_file_name = 'fresh_indices.ini'
        Config.read(config_file_name)
        EXEC_INFO_DIR = Config.get('LocalServerSettings', 'EXEC_INFO_DIR')
        FILL_STRATEGY_ENUM = Config.get('FullReindexSettings', 'FILL_STRATEGY_ENUM')
    except Exception as e:
        print(f"Reading {config_file_name}, got error'{str(e)}'."
              , file=sys.stderr)
        sys.exit(2)

    #
    # Set up a logger in the configured directory for the current execution.
    # N.B. logging.basicConfig only has an effect if the root logger does
    #      not already have handlers configured.
    #
    logging.basicConfig(    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
                            , datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG-1) # KBKBKB @TODO restore logging.INFO
    logger.info(f"logger initialized with effective logging level {logger.getEffectiveLevel()}.")

    try:
        fill_strategy = FillStrategyType[FILL_STRATEGY_ENUM]
        logger.info(f"The fill strategy to be executed is {fill_strategy}.")
    except Exception as e:
        print(  f"\a\nUnable to verify FILL_STRATEGY_ENUM='{FILL_STRATEGY_ENUM}' as"
                f" a valid strategy in the FillStrategyType class.\n"
                , file=sys.stderr)
        sys.exit(3)

    # To avoid running a Flask app, read what is needed from the app.cfg file into a
    # dictionary, without Flask libraries.
    app_cfg_filename = '../../src/instance/app.cfg'
    try:
        appcfg = {}
        try:
            with open(app_cfg_filename, mode="rb") as config_file:
                exec(compile(config_file.read(), app_cfg_filename, "exec"), appcfg)
        except OSError as ose:
            raise f"Unable to load configuration file {app_cfg_filename} due to '{str(ose)}'."
    except Exception as e:
        print(f"Reading {app_cfg_filename}, got error '{str(e)}'."
              , file=sys.stderr)
        sys.exit(2)

def verify_initial_state_for_create(fill_strategy:FillStrategyType, config_indices:dict, es_mgr:ESManager)-> dict:
    op_data = {}
    op_data['index'] = {}
    file_time_prefix = datetime.now().strftime('%Y%m%d')
    op_data['file_time_prefix']=file_time_prefix

    op_data['index'][config_indices['indices']['entities']['public']] = {
        'destination': f"fill{file_time_prefix}_fresh_index_{config_indices['indices']['entities']['public']}"
        , 'max': {
            'last_modified_timestamp': None
            , 'created_timestamp': None
        }
    }
    op_data['index'][config_indices['indices']['entities']['private']] = {
        'destination': f"fill{file_time_prefix}_fresh_index_{config_indices['indices']['entities']['private']}"
        , 'max': {
            'last_modified_timestamp': None
            , 'created_timestamp': None
        }
    }
    op_data['index'][config_indices['indices']['portal']['public']] = {
        'destination': f"fill{file_time_prefix}_fresh_index_{config_indices['indices']['portal']['public']}"
        , 'max': {
            'last_modified_timestamp': None
            , 'created_timestamp': None
        }
    }
    op_data['index'][config_indices['indices']['portal']['private']] = {
        'destination': f"fill{file_time_prefix}_fresh_index_{config_indices['indices']['portal']['private']}"
        , 'max': {
            'last_modified_timestamp': None
            , 'created_timestamp': None
        }
    }

    expectation_errors = []
    for source_index in op_data['index'].keys():
        # Capture source_index document count for storage with op_data
        index_doc_count = es_mgr.get_index_document_count(index_name=source_index)
        logger.debug(f"index {source_index} has {index_doc_count} documents.")
        op_data['index'][source_index]['initial_doc_count']=index_doc_count

        # Capture the newest timestamps of this index for storage in op_data
        for inter_cmd_values_to_capture in ['last_modified_timestamp', 'created_timestamp']:
            try:
                op_data['index'][source_index][AggQueryType.MAX.value][inter_cmd_values_to_capture] = \
                    es_mgr.get_document_agg_value(  index_name=source_index
                                                    , field_name=inter_cmd_values_to_capture
                                                    , agg_name_enum=AggQueryType.MAX)
                if op_data['index'][source_index][AggQueryType.MAX.value][inter_cmd_values_to_capture] is None:
                    expectation_errors.append(f"For the index {source_index}"
                                              f" unable to retrieve the {AggQueryType.MAX.value} '{inter_cmd_values_to_capture}'.")
            except Exception as e:
                expectation_errors.append(f"For the index {source_index}"
                                          f" retrieving the {AggQueryType.MAX.value} '{inter_cmd_values_to_capture}'"
                                          f" caused '{str(e)}'.")
        destination_index = op_data['index'][source_index]['destination']
        if es_mgr.verify_exists(destination_index):
            expectation_errors.append(  f"The index {destination_index}"
                                        f" already exists and should be removed before executing the"
                                        f" fill strategy {fill_strategy}.")
        if not es_mgr.verify_exists(source_index):
            expectation_errors.append(  f"The index {source_index}"
                                        f" does not exist and should be available and filled before executing the"
                                        f" fill strategy {fill_strategy}.")

    if len(expectation_errors) > 0:
        for error in expectation_errors:
            logger.error(error)
        print("Failed to verify initial state of indices.  See logs.",
              file=sys.stderr)
        sys.exit(7)

    # Save the initial state info of each index during this 'create' command to a file, which
    # subsequent commands may use e.g. to "catch up" by re-indexing documents in the source
    # index which changed during the create process.
    try:
        json_op_data_fp = open(f"{EXEC_INFO_DIR}/op_data_{op_data['file_time_prefix']}.json", 'w', encoding="utf-8")
        json.dump(obj=op_data
                  , fp=json_op_data_fp)
        json_op_data_fp.close()
    except Exception as e:
        logger.exception(e)
        print(f"Unable to save state to a file for later commands.  Got error '{str(e)}'.")
        sys.exit(8)

    return op_data

def create_new_indices():
    translator_module = importlib.import_module("hubmap_translator")
    translator_module = translator_module

    # Override the index names loaded from app.cfg which are used for Production by replacing them each with
    # an offline index name which can be filled without interfering with the service until it is ready to be deployed.
    INDICES['indices']['entities']['public'] = op_data['index'][INDICES['indices']['entities']['public']]['destination']
    INDICES['indices']['entities']['private'] = op_data['index'][INDICES['indices']['entities']['private']][
        'destination']
    INDICES['indices']['portal']['public'] = op_data['index'][INDICES['indices']['portal']['public']]['destination']
    INDICES['indices']['portal']['private'] = op_data['index'][INDICES['indices']['portal']['private']]['destination']

    translator = Translator(INDICES, appcfg['APP_CLIENT_ID'], appcfg['APP_CLIENT_SECRET'], token,
                            appcfg['ONTOLOGY_API_BASE_URL'])

    # Skip the uuids comparision step that is only needed for live /reindex-all PUT call
    translator.skip_comparision = True

    auth_helper = translator.init_auth_helper()
    # The second argument indicates to get the groups information
    user_info_dict = auth_helper.getUserInfo(token, True)

    if isinstance(user_info_dict, Response):
        msg = "The given token is expired or invalid"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        sys.exit(msg)

    # Use the new key rather than the 'hmgroupids' which will be deprecated
    group_ids = user_info_dict['group_membership_ids']

    # Ensure the user belongs to the HuBMAP-Data-Admin group
    if not auth_helper.has_data_admin_privs(token):
        msg = "The given token doesn't belong to the HuBMAP-Data-Admin group, access not granted"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        sys.exit(msg)

    start_time = time.time()

    # Before writing the first document to the new indices, create each of them by using the
    # existing search-api configuration and logic.
    for index_group_name in ['entities', 'portal']:
        # mimic translator.delete_and_recreate_indices()
        # get the specific mapping file for the designated index
        group_mapping_file = f"../../src/{INDICES['indices'][index_group_name]['elasticsearch']['mappings']}"
        group_mapping_settings = safe_load((Path(__file__).absolute().parent / group_mapping_file).read_text())
        logger.debug(f"KBKBKB INDICES['indices'][{index_group_name}]={str(INDICES['indices'][index_group_name])}")
        for index_visibility in ['public','private']:
            index_name=INDICES['indices'][index_group_name][index_visibility]
            logger.debug(f"KBKBKB index_name={str(index_name)}")
            esmanager.create_index_unless_exists(index_name, group_mapping_settings)

    logger.info(f"############# Full index via script started at {time.strftime('%H:%M:%S',time.localtime(start_time))} #############")

    translator.translate_full()

    if translator.failed_entity_ids:
        logger.info(f"{len(translator.failed_entity_ids)} entity ids failed")
        print(*translator.failed_entity_ids, sep="\n")
        op_data_supplement['translator_failed_entity_ids']=translator.failed_entity_ids
    else:
        # KBKBKB @TODO delete this whole else clause after development!
        op_data_supplement['translator_failed_entity_ids']=['32323232323232323232323232323232','23232323232323232323232323232323']

    end_time = time.time()
    # KBKBKB @TODO check in with Joe if it is worth it to try determining if threads err'ed and pointing that out here...
    logger.info(f"############# Full index via script complete at {time.strftime('%H:%M:%S',time.localtime(end_time))} #############")

    elapsed_seconds = end_time-start_time
    logger.info(f"############# Full index via script took"
                f" {time.strftime('%H:%M:%S', time.gmtime(elapsed_seconds))}."
                f" #############")

def most_recent_op_data_file():
    # Read operation data saved from the last 'create' command from a file. This 'catch-up' operation will
    # use the index names and timestamps saved for re-index any entities whose ElasticSearch document
    # changed subsequent to the start of the 'create' command.
    search_pattern = os.path.join(EXEC_INFO_DIR, f'op_data_20[0-9][0-9][01][0-9][0-9][0-9]*.json')
    op_data_files = glob.glob(search_pattern)

    if not op_data_files:
        print(  f"No op_data files found in directory '{EXEC_INFO_DIR}'."
                , file=sys.stderr)
        sys.exit(14)

    # Initialize variables to keep track of the most recent file and its modification time
    most_recent_file = None
    most_recent_time = 0

    # Iterate over the files to find the most recently modified one
    for op_data_file in op_data_files:
        try:
            modification_time = os.path.getmtime(op_data_file)
            if modification_time > most_recent_time:
                most_recent_time = modification_time
                most_recent_file = op_data_file
        except FileNotFoundError:
            logger.error("Unable to evaluate the file '{op_data_file}' while searching for most recent op_data.")

    return most_recent_file

def verify_initial_state_for_catch_up(fill_strategy:FillStrategyType, config_indices:dict, es_mgr:ESManager)-> dict:

    most_recent_file = most_recent_op_data_file()

    try:
        json_op_data_fp = open(most_recent_file, 'r')
        op_data = json.load(fp=json_op_data_fp)
        json_op_data_fp.close()
    except Exception as e:
        logger.exception(e)
        print(f"Unable to save state to a file for later commands.  Got error '{str(e)}'.")
        sys.exit(8)

    expectation_errors = []
    for source_index in op_data['index'].keys():
        for timestamp_field in ['last_modified_timestamp', 'created_timestamp']:
            try:
                print(f"KBKBKB op_data['index'][{source_index}][{AggQueryType.MAX.value}][{timestamp_field}]={op_data['index'][source_index][AggQueryType.MAX.value][timestamp_field]}")
                if op_data['index'][source_index][AggQueryType.MAX.value][timestamp_field] is None or \
                    not isinstance(op_data['index'][source_index][AggQueryType.MAX.value][timestamp_field], float):
                    expectation_errors.append(f"Operation data from {most_recent_file}"
                                              f" for the index {source_index}"
                                              f" has a bad value for {AggQueryType.MAX.value} '{timestamp_field}'.")
            except Exception as e:
                expectation_errors.append(f"Using operation data from {most_recent_file}"
                                          f" retrieving the {AggQueryType.MAX.value} '{timestamp_field}'"
                                          f" for the index {source_index}"
                                          f" caused '{str(e)}'.")
        destination_index = op_data['index'][source_index]['destination']
        if not es_mgr.verify_exists(destination_index):
            expectation_errors.append(  f"The index {destination_index}"
                                        f" does not exist, so the operation data in {most_recent_file}"
                                        f" can't be used for a catch-up command.")
        if not es_mgr.verify_exists(source_index):
            expectation_errors.append(  f"The index {source_index}"
                                        f" does not exist, so the operation data in {most_recent_file}"
                                        f" can't be used for a catch-up command.")

    if len(expectation_errors) > 0:
        for error in expectation_errors:
            logger.error(error)
        print("Failed to verify initial state of indices.  See logs.",
              file=sys.stderr)
        sys.exit(7)
    return op_data

# Read the op_data file from the last 'create' command.  While the search-api is down, read each document from
# the source index which was created or updated after the 'create' command started.  Re-index those entities into
# the destination index, so it becomes an exact match of the source index.
def catch_up_new_index():
    print('Not ready to catch-up activity which took place in the Production indices during or after creation of the new indices', file=sys.stderr)
    print('But if we will be once we:', file=sys.stderr)
    print('*** reindex these uuids for documents which failed while creating the new indices:', file=sys.stderr)
    print(f"****** {op_data['translator_failed_entity_ids']}", file=sys.stderr)
    print('*** reindex the uuids for documents have timestamps newer than whatever document was newest in the source index when the new index was created:', file=sys.stderr)
    for source_index in op_data['index'].keys():
        timestamp_range_json_list=[]
        for timestamp_field_name in op_data['index'][source_index][AggQueryType.MAX.value].keys():
            KBKBKB_UNDO_DIVISOR_TO_FAKE_OUT_SOME_UUIDS_TO_REINDEX=1000
            timestamp_value=op_data['index'][source_index][AggQueryType.MAX.value][timestamp_field_name]/KBKBKB_UNDO_DIVISOR_TO_FAKE_OUT_SOME_UUIDS_TO_REINDEX
            timestamp_range_json_list.append(f'{{"range": {{"{timestamp_field_name}": {{"gt": {timestamp_value}}}}}}}')
            timestamp_data = { 'timestamp_field': timestamp_field_name
                               , 'timestamp_op': 'gt'
                               , 'timestamp_value': op_data['index'][source_index][AggQueryType.MAX.value][timestamp_field_name]}
        catch_up_uuids = esmanager.get_document_uuids_by_timestamps(    index_name=source_index
                                                                        , timestamp_data_list=timestamp_range_json_list )
    print(f"****** {catch_up_uuids}", file=sys.stderr)

    sys.exit(14)
    # KBKBKB @TODO recommend QDSL commands to manually do what a 'golive' command should do in the future.


if __name__ == "__main__":

    try:
        command = sys.argv[1]
        if command not in ['create', 'catch-up']:
            print(f"Unexpected command '{command}'.  See help of calling script.")
            sys.exit(13)
    except IndexError as e:
        msg = "Missing command to execute.  See help of calling script"
        logger.exception(msg)
        sys.exit(msg)

    try:
        token = sys.argv[2]
    except IndexError as e:
        msg = "Missing admin group token argument"
        logger.exception(msg)
        sys.exit(msg)

    try:
        init()

        INDICES = safe_load((Path(__file__).absolute().parent.parent / '../src/instance/search-config.yaml').read_text())

        default_index = INDICES['default_index']
        elasticsearch_url = INDICES['indices'][default_index]['elasticsearch']['url'].strip('/')
        eswriter = ESWriter(elasticsearch_url)
        logger.info(f"ESWriter initialized with URL {elasticsearch_url}")
        esmanager = ESManager(elasticsearch_url)
        logger.info(f"ESManager initialized with URL {elasticsearch_url}")

        try:
            if command == 'create':
                op_data = verify_initial_state_for_create(  fill_strategy
                                                            , INDICES
                                                            , esmanager)
            elif command == 'catch-up':
                op_data = verify_initial_state_for_catch_up(    fill_strategy
                                                                , INDICES
                                                                , esmanager)
        except Exception as e:
            logger.exception(e)
            print(f"Unable to verify initial state for command '{command}'. Got error '{str(e)}'."
                  , file=sys.stderr)
            sys.exit(6)
    except Exception as e:
        print(f"\a\nUnable to initialize due to e='{str(e)}'.\n"
              , file=sys.stderr)
        sys.exit(4)

    # Set up a dictionary to hold any op_data which might be tacked on after executing commands.
    op_data_supplement={}

    if command == 'create':
        create_new_indices()
    elif command == 'catch-up':
        catch_up_new_index()

    if op_data_supplement:
        # Tack on any op_data accrued after the initial file was written, and write back to the file.
        most_recent_file = most_recent_op_data_file()

        try:
            json_op_data_fp = open(most_recent_file, 'r')
            filed_op_data = json.load(fp=json_op_data_fp)
            json_op_data_fp.close()
        except Exception as e:
            logger.exception(e)
            print(f"Unable to save state to a file for later commands.  Got error '{str(e)}'.")
            sys.exit(8)
        for key, value in op_data_supplement.items():
            filed_op_data[key]=value

        try:
            json_op_data_fp = open(most_recent_file, 'w', encoding="utf-8")
            json.dump(obj=filed_op_data
                      , fp=json_op_data_fp)
            json_op_data_fp.close()
        except Exception as e:
            logger.exception(e)
            print(f"Unable to updated the saved state to a file for later commands.  Got error '{str(e)}'.")
            sys.exit(8)

    sys.exit(0)

def unused_so_far_index_logged_objects(bucket_name, folder_name, target_index_name, time_elt_name, key_elt_name_list, last_loaded_timestamp=None):
    # Source the JSON extracted from logged events by various processes to
    # various S3 buckets.  Create an OpenSearch document for each JSON entry
    # in a corresponding OSS index.
    logger.info('Begin indexing JSON in S3 to OpenSearch Service indices')

    try:
        s3_worker = S3_connector.S3Worker(theAWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID
                                           , theAWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY
                                           , theAWS_S3_BUCKET_NAME=bucket_name
                                           , theAWS_OBJECT_URL_EXPIRATION_IN_SECS=0)
    except Exception as e:
        logger.exception("Unable to connect to S3 to transfer or verify files.")
        exit(2)

    # Get the existing files in S3
    try:
        folder_obj_list = s3_worker.get_bucket_folder_objects(folder_name_without_delim=folder_name
                                                              ,delimiter=AWS_FOLDER_DELIM)
    except Exception as ke:
        logger.exception(f"Unable to retrieve objects from folder"
                         f" '{bucket_name + AWS_FOLDER_DELIM + folder_name}'"
                         f" in S3 for uploading to OpenSearch Service.")
        exit(2)

    fresh_index_indices: dict = {}
    # Do not include the indexes that are self managed
    for key, value in INDICES['indices'].items():
        if 'reindex_enabled' in value and value['reindex_enabled'] is True:
            fresh_index_indices[key] = value
    indexer = Indexer(indices=fresh_index_indices, default_index=default_index)
    logger.info(f"Indexer initialized")

    logger.info(f"target_index_name={target_index_name}")
    logger.info(f"len(folder_obj_list)={len(folder_obj_list)}")
    # If able to determine the timestamp of the newest document loaded into the
    # index, only look at JSON logged events from that timestamp going forward for
    # addition to the index if they are not already there.
    if last_loaded_timestamp:
        lookback_timestamp = last_loaded_timestamp - timedelta(days=1)
        earliest_load_obj_name = f"gridftp.log-{lookback_timestamp.strftime('%Y%m%d')}.XFER.json"
        logger.info(f"For index {target_index_name},"
                    f" load starting from the S3 entry {earliest_load_obj_name},"
                    f" unless it already exists.")

    for obj in folder_obj_list:
        try:
            obj_yyyymmdd = re.sub(pattern='^.*gridftp.log-', repl='', string=obj['Key'] )
            obj_yyyymmdd = re.sub(pattern='.XFER.json$', repl='', string=obj_yyyymmdd)
            try:
                if last_loaded_timestamp:
                    obj_name_timestamp = datetime.strptime(obj_yyyymmdd
                                                           , '%Y%m%d')
                    if obj_name_timestamp < last_loaded_timestamp:
                        # Skip processing S3 Bucket objects with names indicating they are older than we want to load
                        continue
            except ValueError as ve:
                # when obj['Key'] does not have a "dated name" which can be parsed, assume it needs to be
                # processed as JSON containing a 'log_entries' array.
                pass

            json_body = s3_worker.get_object_body(object_key=obj['Key'])
            logger.debug(f"For Bucket object named {obj['Key']}, got {len(json_body)} byte body"
                         f" to stash each element in target_index_name={target_index_name}")
            doc_dict_list = json.loads(json_body)
            # For now, code the difference in JSON input between API usage parsing and file transfer parsing is
            # indicated by this code fragment.
            # "File transfer" S3 Bucket objects entire JSON payload is to become OpenSearch documents.
            # "API usage" S3 Bucket objects contain a 'log_entries' array as part of their JSON payload, which
            # is to become OpenSearch documents.
            if 'log_entries' in doc_dict_list:    # ...but see if it is "API usage"
                doc_dict_list = doc_dict_list['log_entries']

            for doc_dict in doc_dict_list:
                request_time = datetime.strptime(doc_dict[time_elt_name]
                                                 ,'%Y-%m-%dT%H:%M:%S.%fZ')
                # Skip creating OpenSeach documents for incoming JSON which is older than we want to load
                if last_loaded_timestamp and request_time <= last_loaded_timestamp:
                    continue

                # Some "Transfer stats:" have a TASKID=none.  For those documents, put
                # them in using the destination IP instead as the _id prefix
                es_id_prefix = '_'.join(doc_dict[elt_name] for elt_name in key_elt_name_list)
                es_id_prefix = es_id_prefix.replace('/','__')
                es_id = es_id_prefix+'_'+str(request_time.timestamp())
                doc_json = json.dumps(doc_dict)
                logger.debug(f"\tStash {len(doc_json)} byte document with {len(doc_dict)} fields in index {target_index_name} using _id={es_id}.")
                indexer.index(doc_id=es_id
                              ,document=doc_json
                              ,index_name=target_index_name
                              ,reindex=True)
        except Exception as e:
            logger.exception(f"Unable to retrieve body of S3 object {obj['Key']} for indexing due to e={str(e)}")

# comment further and expand on these...
# Assumptions:
#            : The file transfer log files partition the information, so they never overlap or present superseding info.
#            : This assumption therefore also applies to the S3 Bucket objects for file transfer.
#
#            : The API usage log files each keep the same name, and accumulate info for documents already
#            : in the index and for newer (only) documents to add.  Therefore there is only one S3 Bucket
#            : object per API usage type, and the information to index is in the 'log_entries' element.
#            :
#            : The timestamp of the "newest" document in an OpenSearch index indicates either an
#            : element of the 'log_entries' array of an API usage S3 Bucket JSON object, or an entry in
#            : one of the file transfer S3 Bucket JSON objects.
#            :
#            : The S3 Bucket file transfer JSON object can be identified for a given timestamp, and it can
#            : be assumed that S3 Bucket JSON object was completely loaded if it still in S3.
#
def  unused_so_far_leftover_main_from_log_indexing_takeout():

    logger.info(f"Executing {fill_strategy} to get info parsed from logs and"
                f" stored in S3 into AWS OpenSearch Service indices. ")

    sys.exit(0)

    # Create a list holding dictionaries for each separate OpenSearch index to be filled.
    file_time_prefix = datetime.now().strftime('%Y%m%d')
    swap_dict_list = []
    for composite_index_name in ['entities', 'portal']:
        for es_index_visibility in ['public', 'private']:
            deploy_index_name = INDICES["indices"][composite_index_name][es_index_visibility]
            swap_dict_list.append(
                {'fill_index': f"fill{file_time_prefix}_fresh_index_{deploy_index_name}"
                    , 'composite_index': f"{composite_index_name}"
                    , 'deploy_index': f"{deploy_index_name}"
                    , 'flush_index': f"flush{file_time_prefix}_fresh_index_{deploy_index_name}"
                 }
            )

    # Make sure the indices either exist or do not exist as is
    # appropriate for the fill_strategy configured.
    if fill_strategy in [FillStrategyType.CREATE_FILL_SWAP]:
        for swap_dict in swap_dict_list:
            eswriter.verify_exists(swap_dict['deploy_index'])
            eswriter.verify_exists(swap_dict['fill_index'])
            eswriter.verify_exists(swap_dict['flush_index'])
    elif fill_strategy in [FillStrategyType.CLONE_ADD_SWAP, FillStrategyType.EMPTY_FILL]:
        logger.critical(f"The __main__ method is not set up for the {fill_strategy} fill strategy yet.")
        sys.exit(5)
    else:
        logger.error(f"Unable to verify indices ready for fill_strategy={fill_strategy}")

    # Prepare each target index for the fill_strategy, assure it has
    # no blocks, and wait until it has "green" health.
    if fill_strategy in [FillStrategyType.CREATE_FILL_SWAP]:
        for swap_dict in swap_dict_list:
            # Create a new, empty, temporary index
            eswriter.create_index_unless_exists(swap_dict['fill_index'])
            # Clear any blocks the temporary index may have.
            eswriter.set_index_block(index_name=f"{swap_dict['fill_index']}"
                                     ,block_name=ESWriter.IndexBlockType.NONE)
            # Make sure the temporary index health is "green" before proceeding.
            eswriter.wait_until_index_green(index_name=f"{swap_dict['fill_index']}"
                                            ,wait_in_secs=30)
    elif fill_strategy in [FillStrategyType.CLONE_ADD_SWAP]:
        for swap_dict in swap_dict_list:
            # Just in case they are not blocked for writing already, block writing
            # on the index to be cloned.
            eswriter.set_index_block(index_name=f"{swap_dict['deploy_index']}"
                                     ,block_name=ESWriter.IndexBlockType.WRITE)
            # Clone the existing index to a temporary index, to which more documents can be added
            eswriter.clone_index(source_index_name=swap_dict['deploy_index']
                                 ,target_index_name=swap_dict['fill_index'])
            # After cloning, clear any blocks the temporary index may have.
            eswriter.set_index_block(index_name=f"{swap_dict['fill_index']}"
                                     ,block_name=ESWriter.IndexBlockType.NONE)
            # Make sure the temporary index health is "green" before proceeding.
            eswriter.wait_until_index_green(index_name=f"{swap_dict['fill_index']}"
                                            ,wait_in_secs=30)
    elif fill_strategy in [FillStrategyType.EMPTY_FILL]:
        # For this fill_strategy, skip the fill_index, and empty the
        # deploy_index which will be filled directly, in the code below.
        for swap_dict in swap_dict_list:
            # Create the index to be deployed, if it does not exist alread
            eswriter.create_index_unless_exists(index_name=swap_dict['deploy_index'])
            # Make sure the index to be deployed is emptied of documents
            eswriter.empty_index(swap_dict['deploy_index'])
            # Clear any blocks the index to be deployed may have.
            eswriter.set_index_block(index_name=f"{swap_dict['deploy_index']}"
                                     ,block_name=ESWriter.IndexBlockType.NONE)
            # Make sure the health of index to be deployed is "green" before proceeding.
            eswriter.wait_until_index_green(index_name=f"{swap_dict['deploy_index']}"
                                            ,wait_in_secs=30)
    else:
        logger.error(f"Unable to prepare indices for fill_strategy={fill_strategy}")

    if fill_strategy in [FillStrategyType.CLONE_ADD_SWAP]:
        # Add to each index with the new content in an S3 Bucket
        for swap_dict in swap_dict_list:
            # Determine the "newest" document in the temporary index, then
            # add any documents which are "newer" than that.
            for swap_dict in swap_dict_list:
                most_recent_time_str = eswriter.get_document_agg_value(index_name=swap_dict['deploy_index']
                                                                       ,field_name=swap_dict['time_elt_name']
                                                                       ,agg_name_enum=eswriter.AggQueryType.MAX)
                most_recent_timestamp = datetime.strptime(most_recent_time_str
                                                          ,'%Y-%m-%dT%H:%M:%S.%fZ')
                index_logged_objects(bucket_name=swap_dict['bucket_name']
                                     ,folder_name=swap_dict['bucket_folder_name']
                                     ,target_index_name=swap_dict['fill_index']
                                     ,time_elt_name=swap_dict['time_elt_name']
                                     ,key_elt_name_list=swap_dict['key_elt_list']
                                     ,last_loaded_timestamp=most_recent_timestamp)
            # Make sure the temporary index health is "green" before proceeding.
            eswriter.wait_until_index_green(index_name=f"{swap_dict['fill_index']}"
                                            ,wait_in_secs=30)
    elif fill_strategy in [FillStrategyType.CREATE_FILL_SWAP]:
        # Fill each index with all the associated content in an S3 Bucket
        for swap_dict in swap_dict_list:
            # Fill the temporary index with documents
            index_logged_objects(bucket_name=swap_dict['bucket_name']
                                 ,folder_name=swap_dict['bucket_folder_name']
                                 ,target_index_name=swap_dict['fill_index']
                                 ,time_elt_name=swap_dict['time_elt_name']
                                 ,key_elt_name_list=swap_dict['key_elt_list'])
            # Make sure the temporary index health is "green" before proceeding.
            eswriter.wait_until_index_green(index_name=f"{swap_dict['fill_index']}"
                                            ,wait_in_secs=30)
    elif fill_strategy in [FillStrategyType.EMPTY_FILL]:
        # For this fill_strategy, skip the fill_index, and directly put the
        # associated content of the S3 Bucket into the deploy_index
        for swap_dict in swap_dict_list:
            # Fill the index to be deployed with documents
            index_logged_objects(bucket_name=swap_dict['bucket_name']
                                 ,folder_name=swap_dict['bucket_folder_name']
                                 ,target_index_name=swap_dict['deploy_index']
                                 ,time_elt_name=swap_dict['time_elt_name']
                                 ,key_elt_name_list=swap_dict['key_elt_list'])

            # Make sure the health of index to be deployed is "green" before proceeding.
            eswriter.wait_until_index_green(index_name=f"{swap_dict['deploy_index']}"
                                            ,wait_in_secs=30)
    else:
        logger.error(f"Unable to fill indices for fill_strategy={fill_strategy}")

    if fill_strategy in [FillStrategyType.CLONE_ADD_SWAP
                        ,FillStrategyType.CREATE_FILL_SWAP]:
        # If the fill strategy involved filling a "temporary" index named
        # in swap_dict['fill_index'], use cloning to "rename" things
        # as follows:
        # swap_dict['deploy_index'] cloned to swap_dict['flush_index']
        # swap_dict['fill_index'] cloned to swap_dict['deploy_index']
        # delete swap_dict['fill_index']
        for swap_dict in swap_dict_list:
            # Block writing on the existing index, so it can be cloned once more to
            # an archival index that preserves the state prior to the document additions above.
            eswriter.set_index_block(index_name=f"{swap_dict['deploy_index']}"
                                     ,block_name=ESWriter.IndexBlockType.WRITE)
            # Clone the existing index to an archival index.
            eswriter.clone_index(source_index_name=swap_dict['deploy_index']
                                 ,target_index_name=swap_dict['flush_index'])
            # Make sure the archival index health is "green" before proceeding.
            eswriter.wait_until_index_green(index_name=f"{swap_dict['flush_index']}"
                                            ,wait_in_secs=30)
            # Get rid of the existing index so the temporary index filled above can replace it.
            eswriter.delete_index(index_name=swap_dict['deploy_index'])
            # Block writing on the temporary index, so it can be cloned to become the
            # index to be deployed.
            eswriter.set_index_block(index_name=f"{swap_dict['fill_index']}"
                                     ,block_name=ESWriter.IndexBlockType.WRITE)
            # Clone the temporary index to the index to be deployed
            eswriter.clone_index(source_index_name=swap_dict['fill_index']
                                 ,target_index_name=swap_dict['deploy_index'])
            # Make sure the health of index to be deployed is "green" before proceeding.
            eswriter.wait_until_index_green(index_name=f"{swap_dict['deploy_index']}"
                                            ,wait_in_secs=30)
            # Delete the temporary index, now that its content is in the index to be deployed.
            eswriter.delete_index(index_name=swap_dict['fill_index'])
    elif fill_strategy in [FillStrategyType.EMPTY_FILL]:
        pass
    else:
        logger.error(f"Unable to 'rename' indices for fill_strategy={fill_strategy}")

    logger.info('Finished fill and swap for existing log info OpenSearch Service indices.')
    sys.exit(0)
