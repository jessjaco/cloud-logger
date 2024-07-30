from logging import Formatter, Logger

from cloud_logger import S3AppendBlobHandler


class ALogger(Logger):
    def __init__(self):
        super().__init__(name="test")
        formatter = Formatter()
        handler = S3AppendBlobHandler(formatter, "s3://dep-cl/test_log.txt")
        self.setLevel("INFO")
        self.addHandler(handler)


def test_S3AppendBlobHandler():
    logger = ALogger()
    logger.info("TEST\n")
