import glob
import json
import os
import re
import sys
import configparser
import logging
import importlib
import copy

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
from IndexBlockType import IndexBlockType
from es_manager import ESManager
from TooMuchToCatchUpException import TooMuchToCatchUpException

def init():
    global logger
    global fill_strategy
    global appcfg
    global EXEC_INFO_DIR
    global MAX_EXPECTED_CATCH_UP_UUID_COUNT

    #
    # Read configuration from the INI file and set global constants
    #
    try:
        Config = configparser.ConfigParser()
        config_file_name = 'fresh_indices.ini'
        Config.read(config_file_name)
        EXEC_INFO_DIR = Config.get('LocalServerSettings', 'EXEC_INFO_DIR')
        str_MAX_EXPECTED_CATCH_UP_UUID_COUNT = Config.get('LocalServerSettings', 'MAX_EXPECTED_CATCH_UP_UUID_COUNT')
        MAX_EXPECTED_CATCH_UP_UUID_COUNT=int(str_MAX_EXPECTED_CATCH_UP_UUID_COUNT)
        FILL_STRATEGY_ENUM = Config.get('FullReindexSettings', 'FILL_STRATEGY_ENUM')
    except Exception as e:
        logger.error(f"Reading {config_file_name}, got error'{str(e)}'.")
        sys.exit(2)

    #
    # Set up a logger in the configured directory for the current execution.
    # N.B. logging.basicConfig only has an effect if the root logger does
    #      not already have handlers configured.
    #
    logging.basicConfig(    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
                            , datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.info(f"logger initialized with effective logging level {logger.getEffectiveLevel()}.")

    try:
        fill_strategy = FillStrategyType[FILL_STRATEGY_ENUM]
        logger.info(f"The fill strategy to be executed is {fill_strategy}.")
    except Exception as e:
        logger.error(  f"\a\nUnable to verify FILL_STRATEGY_ENUM='{FILL_STRATEGY_ENUM}' as"
                       f" a valid strategy in the FillStrategyType class.\n")
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
        logger.error(f"Reading {app_cfg_filename}, got error '{str(e)}'.")
        sys.exit(2)

def verify_initial_state_for_create(es_mgr:ESManager, fill_strategy:FillStrategyType, config_indices:dict)-> json:
    global op_data

    # There should be information about one 'create' command in an
    # op_data file, keyed as dictionary entry 0.
    # @TODO-KBKBKB Ask Joe if we want to allow any more operations after 'go-live' for a corresponding 'create', and consider a op_data key that flags no more ops e.g. -1, 256, etc.
    op_data_seq='0'

def verify_initial_state_for_create(es_mgr:ESManager, fill_strategy:FillStrategyType, config_indices:dict)-> json:
    global op_data

    # There should be information about one 'create' command in an
    # op_data file, keyed as dictionary entry 0.
    op_data_seq='0'
    
    file_time_prefix=datetime.now().strftime('%Y%m%d')
    op_data = {op_data_seq:{'command':'create'
                            , 'execution_timestamp': datetime.now()
                            , 'file_time_prefix': file_time_prefix}}

    index_info_dict = {}
    index_info_dict[config_indices['indices']['entities']['public']] = {
        'destination': f"fill{file_time_prefix}_fresh_index_{config_indices['indices']['entities']['public']}"
        , 'max': {
            'last_modified_timestamp': None
            , 'created_timestamp': None
        }
    }
    index_info_dict[config_indices['indices']['entities']['private']] = {
        'destination': f"fill{file_time_prefix}_fresh_index_{config_indices['indices']['entities']['private']}"
        , 'max': {
            'last_modified_timestamp': None
            , 'created_timestamp': None
        }
    }
    index_info_dict[config_indices['indices']['portal']['public']] = {
        'destination': f"fill{file_time_prefix}_fresh_index_{config_indices['indices']['portal']['public']}"
        , 'max': {
            'last_modified_timestamp': None
            , 'created_timestamp': None
        }
    }
    index_info_dict[config_indices['indices']['portal']['private']] = {
        'destination': f"fill{file_time_prefix}_fresh_index_{config_indices['indices']['portal']['private']}"
        , 'max': {
            'last_modified_timestamp': None
            , 'created_timestamp': None
        }
    }

    # If any errors do not meet expectations for initial verification, capture them for
    # logging before giving up on the command.
    expectation_errors = []
    for source_index in index_info_dict.keys():
        # Capture source_index document count for storage with op_data
        index_doc_count = es_mgr.get_index_document_count(index_name=source_index)
        logger.debug(f"index {source_index} has {index_doc_count} documents.")
        index_info_dict[source_index]['initial_doc_count']=index_doc_count

        # Capture the newest timestamps of this index for storage in op_data
        for inter_cmd_values_to_capture in ['last_modified_timestamp', 'created_timestamp']:
            try:
                index_info_dict[source_index][AggQueryType.MAX.value][inter_cmd_values_to_capture] = \
                    es_mgr.get_document_agg_value(  index_name=source_index
                                                    , field_name=inter_cmd_values_to_capture
                                                    , agg_name_enum=AggQueryType.MAX)
                if index_info_dict[source_index][AggQueryType.MAX.value][inter_cmd_values_to_capture] is None:
                    expectation_errors.append(f"For the index {source_index}"
                                              f" unable to retrieve the {AggQueryType.MAX.value} '{inter_cmd_values_to_capture}'.")
            except Exception as e:
                expectation_errors.append(f"For the index {source_index}"
                                          f" retrieving the {AggQueryType.MAX.value} '{inter_cmd_values_to_capture}'"
                                          f" caused '{str(e)}'.")
        destination_index = index_info_dict[source_index]['destination']
        if es_mgr.verify_exists(destination_index):
            expectation_errors.append(  f"The index {destination_index}"
                                        f" already exists and should be removed before executing the"
                                        f" fill strategy {fill_strategy}.")
        if not es_mgr.verify_exists(source_index):
            expectation_errors.append(  f"The index {source_index}"
                                        f" does not exist and should be available and filled before executing the"
                                        f" fill strategy {fill_strategy}.")

    # If any expectations for initial verification were not met, log them and bail out of executing the command.
    if len(expectation_errors) > 0:
        for error in expectation_errors:
            logger.error(error)
        logger.error("Failed to verify initial state of indices.  See logs.")
        sys.exit(7)

    op_data[op_data_seq]['index_info'] = index_info_dict

    # Save the initial state info of each index during this 'create' command to a file, which
    # subsequent commands may use e.g. to "catch up" by re-indexing documents in the source
    # index which changed during the create process.
    try:
        json_op_data_fp = open(f"{EXEC_INFO_DIR}/op_data_{file_time_prefix}.json", 'w', encoding="utf-8")
        json.dump(obj=op_data
                  , fp=json_op_data_fp)
        json_op_data_fp.close()
    except Exception as e:
        logger.exception(e)
        logger.error(f"Unable to save state to a file for later commands.  Got error '{str(e)}'.")
        sys.exit(8)

    return op_data_seq

# Tack on any op_data accrued after the initial file was written, and write back to the file.
def update_op_data_state(op_data_supplement:dict, file_time_prefix:str, op_data_seq:str)->None:
    global op_data
    
    most_recent_file = most_recent_op_data_file()

    # Assume the supplement is keyed by command and adds to the existing data rather than
    # replace any of it.
    for key, value in op_data_supplement.items():
        op_data[op_data_seq]=value | op_data[op_data_seq]

    try:
        json_op_data_fp = open(most_recent_file, 'w', encoding="utf-8")
        json.dump(obj=op_data
                  , fp=json_op_data_fp)
        json_op_data_fp.close()
    except Exception as e:
        logger.exception(e)
        logger.error('Unable to updated the saved state to a file for later commands.  See logs.')
        sys.exit(8)
    return

def get_translator():
    global a_translator
    global op_data

    translator_module = importlib.import_module("hubmap_translator")
    translator_module = translator_module

    # The translator should use the indices names used during the 'create' command, as
    # retrieved from the op_data file.
    index_info_dict=op_data['0']['index_info']
    
    # Override the index names loaded from app.cfg which are used for Production by replacing them each with
    # an offline index name which can be filled without interfering with the service until it is ready to be deployed.
    INDICES['indices']['entities']['public'] = index_info_dict[INDICES['indices']['entities']['public']]['destination']
    INDICES['indices']['entities']['private'] = index_info_dict[INDICES['indices']['entities']['private']]['destination']
    INDICES['indices']['portal']['public'] = index_info_dict[INDICES['indices']['portal']['public']]['destination']
    INDICES['indices']['portal']['private'] = index_info_dict[INDICES['indices']['portal']['private']]['destination']

    a_translator = Translator(INDICES, appcfg['APP_CLIENT_ID'], appcfg['APP_CLIENT_SECRET'], token,
                              appcfg['ONTOLOGY_API_BASE_URL'])

    # Skip the uuids comparision step that is only needed for live /reindex-all PUT call
    a_translator.skip_comparision = True

    auth_helper = a_translator.init_auth_helper()
    # The second argument indicates to get the groups information
    user_info_dict = auth_helper.getUserInfo(token, True)

    if isinstance(user_info_dict, Response):
        msg = "The given token is expired or invalid"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        sys.exit(msg)

    # Ensure the user belongs to the HuBMAP-Data-Admin group
    if not auth_helper.has_data_admin_privs(token):
        msg = "The given token doesn't belong to the HuBMAP-Data-Admin group, access not granted"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        sys.exit(msg)

    return a_translator

def most_recent_op_data_file():
    # Read operation data saved from the last 'create' command from a file. This 'catch-up' operation will
    # use the index names and timestamps saved for re-index any entities whose ElasticSearch document
    # changed subsequent to the start of the 'create' command.
    search_pattern = os.path.join(EXEC_INFO_DIR, f'op_data_20[0-9][0-9][01][0-9][0-9][0-9]*.json')
    op_data_files = glob.glob(search_pattern)

    if not op_data_files:
        logger.debug(  f"No op_data files found in directory '{EXEC_INFO_DIR}'.")
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

def prep_op_data_for_command(es_mgr:ESManager, command:str)-> str:
    global op_data

    current_op_data_file = most_recent_op_data_file()

    logger.info(f"For command '{command}', initializing op_data using current_op_data_file={current_op_data_file}.")

    try:
        json_op_data_fp = open(current_op_data_file, 'r', encoding="utf-8")
        op_data = json.load(fp=json_op_data_fp)
        json_op_data_fp.close()
    except Exception as e:
        msg = f"Unable to get op_data from file '{current_op_data_file}'.  See logs."
        logger.exception(msg)
        sys.exit(msg)
        
    logger.debug(f"Loaded op_data={op_data}")

    # For this op_data file, identify the key of the most recent command, and
    # use a greater number to key information about the current command.
    op_data_seq=str(int(max(op_data.keys()))+1)
    logger.debug(f"Most recent op_data key is {max(op_data.keys())}, and this '{command}' will be saved with key {op_data_seq}")
    cmd_op_data = {'command':command
                   , 'execution_timestamp': datetime.now()}
    op_data[op_data_seq]=cmd_op_data
    
    return op_data_seq

def swap_index_names_per_strategy(es_mgr:ESManager, fill_strategy:FillStrategyType, op_data_key:int)->None:
    global op_data
    global op_data_supplement

    op_data_supplement['golive']['swap_info']=[]

    start_time = time.time()
    logger.info(f"############# Swapping index names to go live via script started at {time.strftime('%H:%M:%S',time.localtime(start_time))} #############")

    logger.info(f"Swapping index names for {fill_strategy}.")

    # Use the indexing information that was recorded for the 'create' command to establish the
    # correct index names to be used in this operation.
    index_info_dict=op_data['0']['index_info']
    for source_index in index_info_dict.keys():
        destination_index = index_info_dict[source_index]['destination']
        logger.info(f"Plan to swap {source_index} to become {destination_index.replace('fill','flush')} and swap {destination_index} to become {source_index}")

    # Make sure the indices either exist or do not exist as is
    # appropriate for the fill_strategy configured.
    if fill_strategy in [FillStrategyType.CREATE_FILL]:
        for source_index in index_info_dict.keys():
            destination_index=index_info_dict[source_index]['destination']
            flush_index=destination_index.replace('fill','flush')
            if not es_mgr.verify_exists(source_index):
                raise Exception(f"Unable to find the source_index '{source_index}' to back up as flush_index '{flush_index}', so cannot swap.")
            if es_mgr.verify_exists(flush_index):
                raise Exception(f"Found existing flush_index '{flush_index}', so cannot backup '{source_index}' to that name, so cannot swap.")
            if not es_mgr.verify_exists(destination_index):
                raise Exception(f"Unable to find destination_index '{destination_index}' to become new '{source_index}', so cannot swap.")
    else:
        logger.error(f"Unable to verify indices ready for fill_strategy={fill_strategy}")

    if fill_strategy in [FillStrategyType.CREATE_FILL]:
        #"rename" things as follows:
        # swap source_index to become "flushYYYYMMDD..." index
        # swap destination_index to become source_index            
        for source_index in index_info_dict.keys():
            destination_index=index_info_dict[source_index]['destination']
            flush_index=destination_index.replace('fill','flush')

            # Block writing on the indices, even though services which write to them should probably be down.
            logger.debug(f"Set {IndexBlockType.WRITE} block on source_index={source_index}.")
            es_mgr.set_index_block(index_name=source_index
                                   , block_name=IndexBlockType.WRITE)
            logger.debug(f"Set {IndexBlockType.WRITE} block on destination_index={destination_index}.")
            es_mgr.set_index_block(index_name=destination_index
                                   , block_name=IndexBlockType.WRITE)
            # Make sure the source_index health is "green" before proceeding.
            es_mgr.wait_until_index_green(index_name=source_index
                                          ,wait_in_secs=30)
            logger.debug(f"Health of source_index={source_index} is green.")

            # Copy the index which the Search API has been using to a "flush" index, then delete it.
            logger.debug(f"Clone source_index={source_index} to flush_index={flush_index}.")
            es_mgr.clone_index(source_index_name=source_index
                               , target_index_name=flush_index)
            op_data_supplement['golive']['swap_info'].append(f"Cloned {source_index} to {flush_index}")
            # Make sure the flush_index health is "green" before proceeding.
            es_mgr.wait_until_index_green(index_name=flush_index
                                            ,wait_in_secs=30)
            logger.debug(f"Health of flush_index={flush_index} is green.")
            logger.debug(f"Set {IndexBlockType.NONE} block on source_index={source_index}.")
            es_mgr.set_index_block(index_name=source_index
                                   , block_name=IndexBlockType.NONE)
            es_mgr.delete_index(index_name=source_index)
            logger.debug(f"Deleted source_index={source_index}.")
            op_data_supplement['golive']['swap_info'].append(f"Deleted {source_index}")

            # Copy the newly created index to an index which the Search API will use, then
            # delete the index which was the destination of 'create' and 'catch-up' operations.
            logger.debug(f"Clone destination_index={destination_index} to source_index={source_index}.")
            es_mgr.clone_index(source_index_name=destination_index
                               , target_index_name=source_index)
            op_data_supplement['golive']['swap_info'].append(f"Cloned {destination_index} to {source_index}")
            # Make sure the source_index health is "green" before proceeding.
            es_mgr.wait_until_index_green(index_name=source_index
                                            ,wait_in_secs=30)
            logger.debug(f"Health of source_index={source_index} is green.")
            logger.debug(f"Set {IndexBlockType.NONE} block on destination_index={destination_index}.")
            es_mgr.set_index_block(index_name=destination_index
                                   , block_name=IndexBlockType.NONE)
            es_mgr.delete_index(index_name=destination_index)
            logger.debug(f"Deleted destination_index={destination_index}.")
            op_data_supplement['golive']['swap_info'].append(f"Deleted {destination_index}")

            # Assure that the index which will be actively used by Search API and the
            # backup of the previous version are writeable.
            logger.debug(f"Set {IndexBlockType.NONE} block on source_index={source_index}.")
            es_mgr.set_index_block(index_name=source_index
                                   , block_name=IndexBlockType.NONE)
            logger.debug(f"Set {IndexBlockType.NONE} block on flush_index={flush_index}.")
            es_mgr.set_index_block(index_name=flush_index
                                   , block_name=IndexBlockType.NONE)
    else:
        logger.error(f"Unable to 'rename' indices for fill_strategy={fill_strategy}")

    logger.info('Finished fill and swap for existing log info OpenSearch Service indices.')

    end_time = time.time()
    logger.info(f"############# Swapping index names to go live via script complete at {time.strftime('%H:%M:%S',time.localtime(end_time))} #############")

    elapsed_seconds = end_time-start_time
    logger.info(f"############# Swapping index names to go live via script took"
                f" {time.strftime('%H:%M:%S', time.gmtime(elapsed_seconds))}."
                f" #############")

# Read the op_data file from the last 'create' command.  While the search-api is down, read each document from
# the source index which was created or updated after the 'create' command started.  Re-index those entities into
# the destination index, so it becomes an exact match of the source index.
def catch_up_new_index(es_mgr:ESManager,op_data_key:int)->None:
    global a_translator
    global op_data
    global op_data_supplement

    start_time = time.time()

    logger.info(f"############# Re-indexing entities of recently touched documents via script started at {time.strftime('%H:%M:%S',time.localtime(start_time))} #############")

    # Go through each index, and identify documents whose timestamps were updated after the
    # last command e.g. after create or after catch-up.
    previous_op_data_seq=str(int(op_data_seq)-1)
    catch_up_uuids=set()
    for source_index in op_data[previous_op_data_seq]['index_info'].keys():
        # Fill a list with dictionaries for each known timestamp to be checked in source_index. Dictionary
        # content should reflect a QDSL query.
        timestamp_range_json_list=[]
        for timestamp_field_name in op_data[previous_op_data_seq]['index_info'][source_index][AggQueryType.MAX.value].keys():
            timestamp_value=op_data[previous_op_data_seq]['index_info'][source_index][AggQueryType.MAX.value][timestamp_field_name]
            logger.debug(f"For source_index={source_index},"
                         f" timestamp_field_name={timestamp_field_name},"
                         f" looking for documents with timestamp_value greater than {timestamp_value}")
            timestamp_range_json_list.append(f'{{"range": {{"{timestamp_field_name}": {{"gt": {timestamp_value}}}}}}}')
        # Query documents with timestamps subsequent to the last command.
        modified_index_uuids=esmanager.get_document_uuids_by_timestamps(index_name=source_index
                                                                        , timestamp_data_list=timestamp_range_json_list )
        logger.debug(f"For source_index={source_index} the touched UUIDs list is {modified_index_uuids}")
        catch_up_uuids |= set(modified_index_uuids)
        
    # Re-index each entity timestamped after the last command.
    logger.info(f"The set of UUIDs for entities to re-index is {str(catch_up_uuids)}")
    op_data_supplement['catchup']['touched_entity_ids']=list(catch_up_uuids)
    for catch_up_uuid in catch_up_uuids:
        logger.debug(f" reindex {catch_up_uuid}")
        a_translator.reindex_entity(uuid=catch_up_uuid)

    if a_translator.failed_entity_ids:
        logger.info(f"{len(a_translator.failed_entity_ids)} entity ids failed")
        logger.debug("\n".join(map(str, a_translator.failed_entity_ids)))
        op_data_supplement['catchup']['translator_failed_entity_ids']=a_translator.failed_entity_ids
    else:
        logger.info(f"No failed_entity_ids reported for the translator.")
        op_data_supplement['catchup']['translator_failed_entity_ids']=[]

    op_data_supplement['catchup']['index_info']={}
    for source_index in op_data[previous_op_data_seq]['index_info'].keys():
        op_data_supplement['catchup']['index_info'][source_index]={}
        # Capture source_index document count for storage with op_data
        index_doc_count = es_mgr.get_index_document_count(index_name=source_index)
        logger.debug(f"index {source_index} has {index_doc_count} documents.")
        op_data_supplement['catchup']['index_info'][source_index]['current_doc_count']=index_doc_count
        op_data_supplement['catchup']['index_info'][source_index]['max']={'last_modified_timestamp': None
                                                                          , 'created_timestamp': None}

        # Capture the newest timestamps of this index for storage in op_data
        for inter_cmd_values_to_capture in ['last_modified_timestamp', 'created_timestamp']:
            try:
                op_data_supplement['catchup']['index_info'][source_index][AggQueryType.MAX.value][inter_cmd_values_to_capture] = \
                    es_mgr.get_document_agg_value(  index_name=source_index
                                                    , field_name=inter_cmd_values_to_capture
                                                    , agg_name_enum=AggQueryType.MAX)
                if op_data_supplement['catchup']['index_info'][source_index][AggQueryType.MAX.value][inter_cmd_values_to_capture] is None:
                    logger.error(f"For the index {source_index}"
                                 f" unable to retrieve the {AggQueryType.MAX.value} '{inter_cmd_values_to_capture}'.")
            except Exception as e:
                logger.error(f"For the index {source_index}"
                             f" retrieving the {AggQueryType.MAX.value} '{inter_cmd_values_to_capture}'"
                             f" caused '{str(e)}'.")

    end_time = time.time()
    # KBKBKB @TODO check in with Joe if it is worth it to try determining if threads err'ed and pointing that out here...
    logger.info(f"############# Re-indexing entities of recently touch documents via script complete at {time.strftime('%H:%M:%S',time.localtime(end_time))} #############")

    elapsed_seconds = end_time-start_time
    logger.info(f"############# Re-indexing via script took"
                f" {time.strftime('%H:%M:%S', time.gmtime(elapsed_seconds))}."
                f" #############")

# Read the op_data file from the last 'create' command.  Read each document from
# the flush index which was created or updated after the 'create' command started.
# Re-index those entities into the new index, even though re-indexing is a more
# expensive operations, so that the new index has everything the flush index had.
def catch_up_live_index(es_mgr:ESManager)->None:
    global op_data
    global op_data_supplement
    global MAX_EXPECTED_CATCH_UP_UUID_COUNT

    # Need to re-identify the indices which are now live for the service.  Rather than extract
    # op_data['0']['index_info'].keys() and parse each key to attempt to match an entry in
    # INDICES, reload from search-config.yaml and make a special set of indices for catching up the
    # active indices the reindex_entity() operation should write to.
    live_indices=copy.deepcopy(INDICES)
    orig_indices = safe_load((Path(__file__).absolute().parent.parent / '../src/instance/search-config.yaml').read_text())

    # Do not use the global INDICES or a_translator which were set up for the 'create' and
    # old catch_up_new_index() method. Create a translator which will write to the active
    # indices in use by Search API
    live_indices['indices']['entities']['public'] = orig_indices['indices']['entities']['public']
    live_indices['indices']['entities']['private'] = orig_indices['indices']['entities']['private']
    live_indices['indices']['portal']['public'] = orig_indices['indices']['portal']['public']
    live_indices['indices']['portal']['private'] = orig_indices['indices']['portal']['private']

    live_translator = Translator(live_indices, appcfg['APP_CLIENT_ID'], appcfg['APP_CLIENT_SECRET'], token,
                              appcfg['ONTOLOGY_API_BASE_URL'])
    
    start_time = time.time()

    logger.info(f"############# Re-indexing entities of recently touched documents via script started at {time.strftime('%H:%M:%S',time.localtime(start_time))} #############")

    # Go through each index, and identify documents whose timestamps were updated after the create command.
    catch_up_uuids=set()
    # At this point, the 'index_info' keys recorded during the 'create' command as "source indices" are now
    # the "active indices" that the Search API uses, and the destination for "catch up" re-index commands.
    for active_index in op_data['0']['index_info'].keys():
        flush_index=op_data['0']['index_info'][active_index]['destination'].replace('fill','flush')
        logger.debug(f"Catch-up index '{active_index}' by re-indexing entities for documents in '{flush_index}' touched after the 'create' command started.")
        
        # Fill a list with dictionaries for each known timestamp to be checked in source_index. Dictionary
        # content should reflect a QDSL query.
        timestamp_range_json_list=[]
        for timestamp_field_name in op_data['0']['index_info'][active_index][AggQueryType.MAX.value].keys():
            timestamp_value=op_data['0']['index_info'][active_index][AggQueryType.MAX.value][timestamp_field_name]
            logger.debug(f"Looking for documents in {flush_index}"
                         f" with timestamp_value greater than {timestamp_value},"
                         f" Captured from timestamp_field_name={timestamp_field_name}"
                         f" of active_index={active_index}"
                         f" during the last operation.")
            timestamp_range_json_list.append(f'{{"range": {{"{timestamp_field_name}": {{"gt": {timestamp_value}}}}}}}')
        # Query documents with timestamps subsequent to the 'create' command which made into the original (now 'flush') index.
        try:
            modified_index_uuids=esmanager.get_document_uuids_by_timestamps(index_name=flush_index
                                                                            , timestamp_data_list=timestamp_range_json_list
                                                                            , expected_max_hits=MAX_EXPECTED_CATCH_UP_UUID_COUNT)
        except TooMuchToCatchUpException as tmtcu:
            msg=f"catch-up command failed due to: {tmtcu.message}"
            logger.critical(msg)
            sys.exit(f"{msg} See logs.")
        logger.debug(f"For flush_index={flush_index} the touched UUIDs list is {modified_index_uuids}")
        catch_up_uuids |= set(modified_index_uuids)
        
    # Re-index each entity timestamped after the last command.
    logger.info(f"The set of UUIDs for entities to re-index is {str(catch_up_uuids)}")
    op_data_supplement['catchup']['touched_entity_ids']=list(catch_up_uuids)
    for catch_up_uuid in catch_up_uuids:
        logger.debug(f" reindex {catch_up_uuid}")
        live_translator.reindex_entity(uuid=catch_up_uuid)

    if live_translator.failed_entity_ids:
        logger.info(f"{len(live_translator.failed_entity_ids)} entity ids failed")
        logger.debug("\n".join(map(str, live_translator.failed_entity_ids)))
        op_data_supplement['catchup']['translator_failed_entity_ids']=live_translator.failed_entity_ids
    else:
        logger.info(f"No failed_entity_ids reported for the translator.")
        op_data_supplement['catchup']['translator_failed_entity_ids']=[]

    op_data_supplement['catchup']['index_info']={}
    for active_index in op_data['0']['index_info'].keys():
        op_data_supplement['catchup']['index_info'][active_index]={}
        # Capture active_index document count for storage with op_data
        index_doc_count = es_mgr.get_index_document_count(index_name=active_index)
        logger.debug(f"index {active_index} has {index_doc_count} documents.")
        op_data_supplement['catchup']['index_info'][active_index]['current_doc_count']=index_doc_count
        op_data_supplement['catchup']['index_info'][active_index]['max']={'last_modified_timestamp': None
                                                                          , 'created_timestamp': None}

        # Capture the newest timestamps of this index for storage in op_data
        for inter_cmd_values_to_capture in ['last_modified_timestamp', 'created_timestamp']:
            try:
                op_data_supplement['catchup']['index_info'][active_index][AggQueryType.MAX.value][inter_cmd_values_to_capture] = \
                    es_mgr.get_document_agg_value(  index_name=active_index
                                                    , field_name=inter_cmd_values_to_capture
                                                    , agg_name_enum=AggQueryType.MAX)
                if op_data_supplement['catchup']['index_info'][active_index][AggQueryType.MAX.value][inter_cmd_values_to_capture] is None:
                    logger.error(f"For the index {active_index}"
                                 f" unable to retrieve the {AggQueryType.MAX.value} '{inter_cmd_values_to_capture}'.")
            except Exception as e:
                logger.error(f"For the index {active_index}"
                             f" retrieving the {AggQueryType.MAX.value} '{inter_cmd_values_to_capture}'"
                             f" caused '{str(e)}'.")

    end_time = time.time()
    # KBKBKB @TODO check in with Joe if it is worth it to try determining if threads err'ed and pointing that out here...
    logger.info(f"############# Re-indexing entities of recently touch documents via script complete at {time.strftime('%H:%M:%S',time.localtime(end_time))} #############")

    elapsed_seconds = end_time-start_time
    logger.info(f"############# Re-indexing via script took"
                f" {time.strftime('%H:%M:%S', time.gmtime(elapsed_seconds))}."
                f" #############")

def create_new_indices():
    global a_translator
    global op_data_supplement

    start_time = time.time()

    # Before writing the first document to the new indices, create each of them by using the
    # existing search-api configuration and logic.
    for index_group_name in ['entities', 'portal']:
        # mimic a_translator.delete_and_recreate_indices()
        # get the specific mapping file for the designated index
        group_mapping_file = f"../../src/{INDICES['indices'][index_group_name]['elasticsearch']['mappings']}"
        group_mapping_settings = safe_load((Path(__file__).absolute().parent / group_mapping_file).read_text())
        for index_visibility in ['public','private']:
            index_name=INDICES['indices'][index_group_name][index_visibility]
            esmanager.create_index_unless_exists(index_name, group_mapping_settings)

    logger.info(f"############# Full index via script started at {time.strftime('%H:%M:%S',time.localtime(start_time))} #############")

    a_translator.translate_full()

    if a_translator.failed_entity_ids:
        logger.info(f"{len(a_translator.failed_entity_ids)} entity ids failed")
        logger.debug("\n".join(map(str, a_translator.failed_entity_ids)))
        op_data_supplement['create']['translator_failed_entity_ids']=a_translator.failed_entity_ids
    else:
        logger.info(f"No failed_entity_ids reported for the translator.")
        op_data_supplement['create']['translator_failed_entity_ids']=[]

    end_time = time.time()
    # KBKBKB @TODO check in with Joe if it is worth it to try determining if threads err'ed and pointing that out here...
    logger.info(f"############# Full index via script complete at {time.strftime('%H:%M:%S',time.localtime(end_time))} #############")

    elapsed_seconds = end_time-start_time
    logger.info(f"############# Full index via script took"
                f" {time.strftime('%H:%M:%S', time.gmtime(elapsed_seconds))}."
                f" #############")

if __name__ == "__main__":
    global a_translator
    global op_data
    global op_data_supplement

    try:
        command = sys.argv[1]
        if command not in ['create', 'catch-up', 'go-live']:
            logger.error(f"Unexpected command '{command}'.  See help of calling script.")
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

        if ('op_data' in globals() or 'op_data' in locals()) and op_data:
            logger.debug(f"Before verifying initial state, op_data={op_data}")
        else:
            logger.debug(f"No op_data before verifying initial state.")
        try:
            if command == 'create':
                op_data_seq = verify_initial_state_for_create(es_mgr=esmanager
                                                              , fill_strategy=fill_strategy
                                                              , config_indices=INDICES)
            elif command == 'catch-up':
                op_data_seq = prep_op_data_for_command(es_mgr=esmanager
                                                       , command=command)
            elif command == 'go-live':
                op_data_seq = prep_op_data_for_command(es_mgr=esmanager
                                                       , command=command)

        except Exception as e:
            logger.exception(e)
            logger.error(f"Unable to verify initial state for command '{command}'. Got error '{str(e)}'.")
            sys.exit(6)
        if ('op_data' in globals() or 'op_data' in locals()) and op_data:
            logger.debug(f"After verifying initial state, op_data={op_data}")
        else:
            logger.debug(f"No op_data after verifying initial state.")
        logger.debug(f"After verifying initial state, op_data_seq={op_data_seq}.")
    except Exception as e:
        logger.exception(e)
        logger.error(f"\a\nUnable to initialize due to e='{str(e)}'.\n")
        sys.exit(4)

    try:
        a_translator=get_translator()
    except Exception as e:
        msg = 'Initializing a translator failed.'
        logger.exception(msg)
        sys.exit(f"{msg} See logs.")
        
    # Set up a dictionary to hold any op_data which might be tacked on after executing commands.
    op_data_supplement={}

    if command == 'create':
        op_data_supplement['create']={}
        create_new_indices()
        print('#############')
        print('Completed create command.')
        print(f"Next either take down the service and execute the 'catch-up'"
              f" command, or execute 'catch-up' with the service up, knowing"
              f" is may need to be executed again if documents are being"
              f" written to the indices right now.")
        print('#############')
    elif command == 'catch-up':
        op_data_supplement['catchup']={}
        # catch_up_new_index(es_mgr=esmanager
        #                    , op_data_key=op_data_seq)
        catch_up_live_index(es_mgr=esmanager)
        print('#############')
        print('Completed catch-up command.')
        print(f"Next either take down the service and catch-up again before"
              f" executing 'go-live', or proceed to execute 'go-live' if"
              f" no new documents are being written to the indices right now.")
        print('#############')
    elif command == 'go-live':
        op_data_supplement['golive']={}
        swap_index_names_per_strategy(es_mgr=esmanager
                                      , fill_strategy=fill_strategy
                                      , op_data_key=op_data_seq)
        print('#############')
        print('Completed go-live command.')
        print(f"You may want to visually verify all ElasticSearch indices have 'green health' in AWS."
              f" If services were brought down to execute this command, they can be brought back up.")
        print('#############')

    # If executing the preceding commands generated any extra operational data to be externalized for
    # fresh index operations, add to the file of op_data for the orignal 'create' command.
    try:
        if op_data_supplement:
            update_op_data_state(op_data_supplement=op_data_supplement
                                 , file_time_prefix=op_data['0']['file_time_prefix']
                                 , op_data_seq=op_data_seq)
    except Exception as e:
        logger.exception(e)
        logger.error(f"Unable to update the op_data state after command '{command}'. Got error '{str(e)}'.")
        sys.exit(6)
        
    sys.exit(0)
