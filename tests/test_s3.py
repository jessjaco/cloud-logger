from logging import Formatter, Logger

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
    logger.info("test")
    assert logger.cloud_handler.log_exists()


def test_CsvLogger_overwrite_empties_log():
    path = "dep-cl/test_logger_overwrite.txt"
    first = CsvLogger("first", path=path, cloud_handler=S3Handler, overwrite=True)
    first.info("old entry")

    second = CsvLogger(
        "second", path=path, cloud_handler=S3Handler, overwrite=True, header="h\n"
    )
    assert second.cloud_handler._s3.cat_file(second.cloud_handler.path) == b"h\n"
