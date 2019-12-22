"""
    trod.db.log
    ~~~~~~~~~~~
"""
import logging


logging.basicConfig(
    format='[%(asctime)s] [%(process)d] [%(levelname)s] %(message)s',
)
logger = logging.getLogger('trod')  # pylint: disable=invalid-name
