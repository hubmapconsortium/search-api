import logging

logger = logging.getLogger(__name__)


def _log_transformation_error(doc, msg):
    doc['transformation_errors'] = doc.get('transformation_errors', [])
    doc['transformation_errors'].append(msg)
    logger.info(f"doc={doc.get('uuid')}:, {msg}")
