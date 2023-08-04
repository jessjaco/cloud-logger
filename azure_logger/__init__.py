from logging import Formatter, Handler, Logger
from typing import Union

from azure.storage.blob import ContainerClient
import pandas as pd


class AzureAppendBlobHandler(Handler):
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

    def emit(self, data):
        self.write(self.format(data))

    def write(self, data):
        self._blob_client.append_block(data)


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
        container_client: ContainerClient,
        path: str,
        overwrite: bool = True,
        header: Union[str, None] = None,
        fmt: str = "%(asctime)s|%(message)s\n",
        datefmt: str = "%Y-%m-%d %H:%M:%S",
        delimiter: str = "|",
    ):
        super().__init__(name)

        self.container_client = container_client
        self.delimiter = delimiter
        self.path = path

        formatter = CsvFormatter(fmt, datefmt, delimiter=delimiter)
        appending = container_client.get_blob_client(path).exists() and not overwrite
        handler = AzureAppendBlobHandler(
            formatter, container_client, path, overwrite=overwrite
        )

        self.addHandler(handler)

        if header and not appending:
            handler.write(header)

    def parse_log(self) -> pd.DataFrame:
        blob_client = self.container_client.get_blob_client(self.path)
        return pd.read_csv(blob_client.url, sep=self.delimiter, skipinitialspace=True)
