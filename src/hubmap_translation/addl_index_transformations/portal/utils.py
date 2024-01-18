import logging

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def _log_transformation_error(doc, msg):
    doc['transformation_errors'].append(msg)
    logger.info(msg)
