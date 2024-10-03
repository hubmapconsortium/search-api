import requests
import json
import logging
from IndexBlockType import IndexBlockType
from AggQueryType import AggQueryType

# Set logging format and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
#logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s:%(lineno)d: %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Class to handle tasks in ElasticSearch outside the usual create, read, update, and delete tasks for
# documents, which are handled by ESWriter
class ESManager:
    def __init__(self, elasticsearch_url):
        self.elasticsearch_url = elasticsearch_url

    # Get a supported aggregate value of a document field, such as the newest or oldest date of
    # some timestamp in the doc.
    def get_document_agg_value(self, index_name, field_name, agg_name_enum: AggQueryType) -> str:
        if agg_name_enum not in AggQueryType:
            logger.error(f"In ESManager.get_document_agg_value() with index_name='{index_name}'"
                         f" and field_name='{field_name}',"
                         f" agg_name_enum='{agg_name_enum}' is not a supported aggregation.")
            raise Exception(f"agg_name_enum='{agg_name_enum}' is not a supported aggregation.")

        headers = {'Content-Type': 'application/json'}
        agg_field_query = f'{{ "aggs": {{"agg_query_result": {{"{agg_name_enum}": {{"field": "{field_name}"}}}}}}}}'
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_search?size=0"
                                 ,headers=headers
                                 ,data=agg_field_query)
            if rspn.ok:
                rspn_json = json.loads(rspn.text)
                value = rspn_json['aggregations']['agg_query_result']['value']
                if value is None or \
                   'value' not in rspn_json['aggregations']['agg_query_result']:
                    # It is expected we will get here if the index has zero entries during development, but
                    # no special handling for that situation.  Assume indices will have one or more documents in
                    # other situations, and log the lack of result as an error.
                    msg = f"Unable to aggregate on agg_name_enum='{agg_name_enum}', field_name='{field_name}'"
                    logger.error(msg)
                    raise Exception(msg)
                return rspn_json['aggregations']['agg_query_result']['value']
            else:
                logger.error(f"Aggregate query {agg_name_enum}"
                             f" failed on index: {index_name}"
                             f" for field_name={field_name}:")
                logger.error(f"Error Message: {rspn.text}")
        except Exception as e:
            msg = f"Exception encountered executing ESManager.get_document_agg_value()" \
                  f" with index_name='{index_name}'," \
                  f" and field_name={field_name}:"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    # Get a count of the documents in an index
    def get_index_document_count(self, index_name) -> int:
        try:
            count_query = f"{self.elasticsearch_url}/{index_name}/_count"
            rspn = requests.get(count_query)
            if rspn.ok:
                rspn_json = json.loads(rspn.text)
                value = rspn_json['count']
                if value is None or \
                   'count' not in rspn_json:
                    msg = f"Unable to get the document count for '{index_name}'."
                    logger.error(msg)
                    raise Exception(msg)
                return int(rspn_json['count'])
            else:
                logger.error(f"Count query {count_query} failed:")
                logger.error(f"Error Message: {rspn.text}")
        except Exception as e:
            msg = f"Exception encountered executing ESManager.get_index_document_count()" \
                  f" with count query '{count_query}':"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def get_document_uuids_by_timestamps(self, index_name:str, timestamp_data_list:list ) -> list:
        replacement_str=','.join(timestamp_data_list)
        query_json='''{
                        "query": {
                            "bool": {
                                "should": [
                                    TIME_STAMP_RANGES_GO_HERE
                                ],
                                "minimum_should_match": 1
                            }
                        },
                        "_source": false
                    }'''
        query_json=query_json.replace('TIME_STAMP_RANGES_GO_HERE', replacement_str)
        headers = {'Content-Type': 'application/json'}
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_search"
                                 ,headers=headers
                                 ,data=query_json)
            if rspn.ok:
                post_create_revised_uuids = []
                rspn_json=rspn.json()

                if 'hits' in rspn_json and 'hits' in rspn_json['hits']:
                    for hit in rspn_json['hits']['hits']:
                        post_create_revised_uuids.append(hit['_id'])
                logger.info(f"Search of {index_name}"
                            f" returned {len(post_create_revised_uuids)} UUIDs"
                            f" revised after the specified timestamp")
                return post_create_revised_uuids
            else:
                logger.error(f"Search of {index_name} for post-create revised documents failed:")
                logger.error(f"Error Message: {rspn.text}")
        except Exception as e:
            msg = f"Exception encountered executing ESManager.get_document_uuids_by_timestamps()" \
                  f" with query_json '{query_json}':"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            
    def delete_index(self, index_name):
        try:
            rspn = requests.delete(url=f"{self.elasticsearch_url}/{index_name}")

            if rspn.ok:
                logger.info(f"Deleted index: {index_name}")
            else:
                logger.error(f"Failed to delete index: {index_name} in elasticsearch.")
                logger.error(f"Error Message: {rspn.text}")
        except Exception:
            msg = "Exception encountered executing ESManager.delete_index()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    # The settings and mappings definition needs to be passed in via config
    def create_index(self, index_name, config):
        try:
            headers = {'Content-Type': 'application/json'}

            rspn = requests.put(f"{self.elasticsearch_url}/{index_name}", headers=headers, data=json.dumps(config))
            if rspn.ok:
                logger.info(f"Created index: {index_name}")
            else:
                logger.error(f"Failed to create index: {index_name} in elasticsearch.")
                logger.error(f"Error Message: {rspn.text}")
                raise Exception(f"Failed to create index: {index_name} in"
                                f" elasticsearch due to {rspn.text}")
        except Exception as e:
            msg = "Exception encountered during executing ESManager.create_index()."
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise e

    def create_index_unless_exists(self, index_name, index_mapping_settings):
        exists_rspn = requests.head(url=f"{self.elasticsearch_url}/{index_name}")
        if exists_rspn.ok:
            logger.debug(f"Not creating index_name={index_name} because it already exists.")
            return
        logger.debug(f"Creating index_name={index_name}.")
        self.create_index(  index_name=index_name
                            , config=index_mapping_settings)

    # Expect an HTTP 200 response if index_name exists, or a 404 if it does not exist
    def verify_exists(self, index_name):
        rspn=requests.head(url=f"{self.elasticsearch_url}/{index_name}")
        return rspn.status_code in [200]

    def empty_index(self, index_name):
        headers = {'Content-Type': 'application/json'}
        match_all_query = '{ "query": { "match_all": {} } }'
        try:
            rspn = requests.post(f"{self.elasticsearch_url}/{index_name}/_delete_by_query?conflicts=proceed"
                                 ,headers=headers
                                 ,data=match_all_query)
            if rspn.ok:
                logger.info(f"Emptied index: {index_name}")
            else:
                logger.error(f"Failed to empty index: {index_name} in elasticsearch.")
                logger.error(f"Error Message: {rspn.text}")
        except Exception:
            msg = "Exception encountered executing ESManager.empty_index()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    # Use the dedicated API to set a recognized block on an index.
    # N.B. Such blocks are undone using dynamic index settings rather than with a dedicated API
    # e.g. PUT your_index/_settings {"index": {"blocks.read_only": false}}
    # https://opensearch.org/docs/latest/api-reference/cluster-api/cluster-settings/
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-blocks.html
    def set_index_block(self, index_name, block_name):
        if block_name not in IndexBlockType:
            raise ValueError(f"'{block_name}' is not a block name supported by IndexBlockType")
        try:
            if block_name is IndexBlockType.NONE:
                headers = {'Content-Type': 'application/json'}
                payload_json = '{"index": {"blocks.write": false, "blocks.read_only": false,  "blocks.read_only_allow_delete": false}}'
                rspn = requests.put(url=f"{self.elasticsearch_url}/{index_name}/_settings"
                                    ,headers=headers
                                    ,data=payload_json)
            else:
                rspn = requests.put(url=f"{self.elasticsearch_url}/{index_name}/_block/{block_name}")
        except Exception as e:
            msg = "Exception encountered during executing ESManager.set_index_block()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise e

        response_dict = json.loads(rspn.text)
        if rspn.status_code in [200, 201, 202] and 'acknowledged' in response_dict and response_dict['acknowledged']:
            # {
            #     "acknowledged": true,
            #     "shards_acknowledged": true,
            #     "indices": [{
            #         "name": "my-index-000001",
            #         "blocked": true
            #     }]
            # }
            logger.info(f"Set '{block_name}' block on index: {index_name}")
            return
        else:
            logger.error(f"Failed to set '{block_name}' block on index: {index_name}")
            logger.error(f"Error Message: {rspn.text}")
            raise Exception(f"Failed to set '{block_name}' block on"
                            f" index: {index_name}, with"
                            f" status_code {rspn.status_code}.  See logs.")

    # Use the Clone API of OpenSearch to clone the source index to the target
    # https://opensearch.org/docs/latest/api-reference/index-apis/clone/
    def clone_index(self, source_index_name, target_index_name):
        # Clone the source index into the target index, and set the target to read/write mode.
        try:
            rspn = requests.put(url=f"{self.elasticsearch_url}/{source_index_name}/_clone/{target_index_name}")
        except Exception as e:
            msg = f"During clone_index('{source_index_name}', '{target_index_name}')," \
                  f" encountered {e.__class__} exception."
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise Exception(f"Failed to clone source index: {source_index_name} "
                            f" to target index: {target_index_name} due to"
                            f" {e.__class__} exception.  See logs.")

        if rspn.ok:
            logger.info(f"Cloned source index: {source_index_name} to target index: {target_index_name}")
        else:
            logger.error(f"Failed to clone source index: {source_index_name} "
                         f" to target index: {target_index_name}.")
            logger.error(f"Error Payload: {rspn.text}")
            response_error_list = json.loads(rspn.text)
            raise Exception(f"Failed to clone source index: {source_index_name} "
                            f" to target index: {target_index_name} with"
                            f" status_code-{response_error_list['status']},"
                            f" reason-{response_error_list['error']['reason']}. See logs.")

    # Wait for the target index to be "green" or the wait_in_seconds to expire. Raise
    # an exception only if the target index is not ready
    # https://opensearch.org/docs/1.2/opensearch/rest-api/cluster-health/
    def wait_until_index_green(self, index_name, wait_in_secs):
        # GET /_cluster/health/target_index?wait_for_status=green&timeout=30s
        try:
            rspn = requests.get(f"{self.elasticsearch_url}/_cluster/health/{index_name}?"
                                f"wait_for_status=green&"
                                f"timeout={wait_in_secs}s")
        except Exception as e:
            msg = f"During wait_until_index_green('{index_name}', '{wait_in_secs}')," \
                  f" encountered {e.__class__} exception."
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise Exception(f"Failed during wait for index: {index_name} "
                            f" to reach \"green\" health within {wait_in_secs} seconds due to"
                            f" {e.__class__} exception.  See logs.")

        if rspn.ok:
            logger.debug(f"Wait for index: {index_name} to reach \"green\" health complete.")
            return
        else:
            response_error_list = json.loads(rspn.text)
            if response_error_list['timed_out'] or response_error_list['status'] != 'green':

                logger.error(f"Failed to get \"green\" health for index: {index_name} "
                             f" within {wait_in_secs} seconds.")
                logger.error(f"Error Payload: {rspn.text}")
                raise Exception(f"Failed to get \"green\" health for index: {index_name} "
                                f" within {wait_in_secs} seconds. See logs.")
            else:
                # Do not expect to reach here, given statement at
                # https://opensearch.org/docs/1.2/opensearch/rest-api/cluster-health/#example
                logger.error('Unexpectedly got a non-ok response, for reasons not expected in the response body.')
                logger.error(f"Error Payload: {rspn.text}")
