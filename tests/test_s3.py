from logging import Formatter, Logger

import s3fs

from cloud_logger import S3Handler, CsvLogger


class ALogger(Logger):
    def __init__(self):
        super().__init__(name="test")
        formatter = Formatter()
        handler = S3Handler(formatter, "s3://dep-cl/test_log.txt")
        self.setLevel("INFO")
        self.addHandler(handler)


def test_S3AppendBlobHandler():
    logger = ALogger()
    logger.info("TEST\n")


def test_CsvLogger():
    logger = CsvLogger(
        "test", path="dep-cl/test_logger.txt", cloud_handler=S3Handler, overwrite=True
    )
    assert not logger.cloud_handler.log_exists()
    logger.info("test")
    assert logger.cloud_handler.log_exists()
