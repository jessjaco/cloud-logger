from logging import Formatter, Handler, Logger

from azure.storage.blob import ContainerClient


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
        header: str = None,
        fmt: str = "%(asctime)s | %(message)s\n",
        datefmt: str = "%Y-%m-%d %H:%M:%S",
    ):
        super().__init__(name)

        formatter = CsvFormatter(fmt, datefmt, delimiter="|")
        handler = AzureAppendBlobHandler(
            formatter, container_client, path, overwrite=True
        )

        self.addHandler(handler)

        if header:
            handler.write(header)
