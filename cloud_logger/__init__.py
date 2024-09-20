from ast import literal_eval
from logging import Formatter, Handler, Logger, INFO
from typing import Callable

from aiobotocore.session import AioSession
from azure.storage.blob import ContainerClient
import pandas as pd
import s3fs


class CloudHandler(Handler):
    def log_exists(self):
        pass


class AzureAppendBlobHandler(CloudHandler):
    def __init__(
        self,
        formatter: Formatter,
        container_client: ContainerClient,
        path: str,
        overwrite: bool = False,
    ):
        super().__init__()

        self.formatter = formatter

        self._blob_client = container_client.get_blob_client(path)
        if not self._blob_client.exists() or overwrite:
            self._blob_client.create_append_blob()
        self.path = self._blob_client.url

    def log_exists(self):
        return self._blob_client.exists

    def emit(self, data):
        self.write(self.format(data))

    def write(self, data):
        self._blob_client.append_block(data)


class S3Handler(CloudHandler):
    def __init__(
        self, formatter: Formatter, path: str, overwrite: bool = False, **kwargs
    ):
        super().__init__()

        self.formatter = formatter
        self.path = f"s3://{path}"
        self._s3 = s3fs.S3FileSystem(anon=False, **kwargs)
        if overwrite and self.log_exists():
            self._s3.rm_file(path)

    def log_exists(self):
        return self._s3.exists(self.path)

    def emit(self, data):
        self.write(self.format(data))

    def write(self, data):
        with self._s3.open(self.path, "a") as f:
            f.write(data)


class CsvFormatter(Formatter):
    def __init__(self, fmt, datefmt, delimiter=","):
        super().__init__(fmt, datefmt)
        self._delimiter = delimiter

    def format_msg(self, msg):
        if isinstance(msg, list):
            msg = self._delimiter.join(map(str, msg))
        return msg

    def format(self, record):
        record.msg = self.format_msg(record.msg)
        return super().format(record)


class CsvLogger(Logger):
    def __init__(
        self,
        name: str,
        path: str,
        overwrite: bool = True,
        header: str | None = None,
        fmt: str = "%(asctime)s|%(message)s\n",
        datefmt: str = "%Y-%m-%d %H:%M:%S",
        delimiter: str = "|",
        cloud_handler: Callable = S3Handler,
        **kwargs,
    ):
        super().__init__(name)

        self.delimiter = delimiter
        self.path = path

        formatter = CsvFormatter(fmt, datefmt, delimiter=delimiter)
        handler = cloud_handler(formatter, path, overwrite=overwrite, **kwargs)
        self.cloud_handler = handler

        self.addHandler(handler)
        self.setLevel(INFO)
        appending = self.cloud_handler.log_exists() and not overwrite
        if header and not appending:
            handler.write(header)

    def parse_log(self) -> pd.DataFrame | None:
        return pd.read_csv(
            self.cloud_handler.path, sep=self.delimiter, skipinitialspace=True
        )

    def filter_by_log(self, df: pd.DataFrame) -> pd.DataFrame:
        log = self.parse_log().set_index("index")
        log.index = [literal_eval(i) for i in log.index]

        return df[~df.index.isin(log.index)]


def filter_by_log(
    df: pd.DataFrame, log: pd.DataFrame, retry_errors: bool = True
) -> pd.DataFrame:
    # Need to decide if this is where we do this. I want to keep the logger
    # fairly generic. Suppose we could subclass it.
    log = log.set_index("index")
    log.index = [literal_eval(i) for i in log.index]

    task_bool = df.index.isin(log.index)

    if retry_errors:
        completes = log.index[log.status == "complete"]
        task_bool = task_bool & df.index.isin(completes)

    return df[~task_bool]
