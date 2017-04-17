# Python Imports
import logging


def setup_logger(verbose=True):
    # create console handler and set level to info
    root_log_level = logging.DEBUG if verbose else logging.INFO
    root_logger = logging.getLogger('s3_upload')
    root_logger.setLevel(root_log_level)
    handler = logging.StreamHandler()
    handler.setLevel(root_log_level)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


if __name__ == "__main__":
    setup_logger()
    from . import db
    db.export()
