from . import libdataproxy
from . import dp_pb2 as proto

class DataProxyFileAdapter:
    def __init__(self, config: proto.DataProxyConfig):
        self.data_proxy_file = libdataproxy.DataProxyFile(config.SerializeToString())

    def close(self):
        self.data_proxy_file.close()

    def download_file(self, info: proto.DownloadInfo, file_path: str, file_format: proto.FileFormat):
        self.data_proxy_file.download_file(info.SerializeToString(), file_path, file_format)

    def upload_file(self, info: proto.UploadInfo, file_path: str, file_format: proto.FileFormat):
        self.data_proxy_file.upload_file(info.SerializeToString(), file_path, file_format)