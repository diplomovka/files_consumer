from .file_data import FileData
from typing import List

class FileDataList:
    def __init__(self, files:List[FileData], last_file:bool, file_num:int):
        self.files = files
        self.last_file = last_file
        self.file_num = file_num

    def to_dict(self):
        return {
            "files": [file.to_dict() for file in self.files],
            "last_file": self.last_file,
            "file_num": self.file_num
        }