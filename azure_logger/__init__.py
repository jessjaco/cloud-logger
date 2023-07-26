from logging import Formatter, Handler, Logger

from azure.storage.blob import ContainerClient


class AzureAppendBlobHandler(Handler):
    def __init__(self, container_client: ContainerClient, path: str):
        super().__init__()

        self._blob_client = container_client.get_blob_client(path)
        if not self._blob_client.exists():
            self._blob_client.create_append_blob()

    def emit(self, data):
        self._blob_client.append_block(self.format(data))


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
        return Formatter.format(self, record)


class CsvLogger(Logger):
    def __init__(
        self,
        name: str,
        container_client: ContainerClient,
        path: str,
        header=None,
    ):
        super().__init__(name)

        if header and isinstance(header, str):
            header = header.split(",")
        self.header = header

        handler = AzureAppendBlobHandler(container_client, path)
        self.addHandler(handler)
