import logging

logger = logging.getLogger(__name__)

# (connect, read) timeout in seconds for calls to other services.
REQUEST_TIMEOUT = (10, 120)


def _log_transformation_error(doc, msg):
    doc['transformation_errors'] = doc.get('transformation_errors', [])
    doc['transformation_errors'].append(msg)
    logger.info(f"doc={doc.get('uuid')}:, {msg}")
