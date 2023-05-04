class FileData:
    def __init__(self, file_name, data, data_hash, experiment_name):
        self.file_name = file_name
        self.data = data
        self.data_hash = data_hash
        self.experiment_name = experiment_name

    def to_dict(self):
        return {
            "file_name": self.file_name,
            "data": self.data,
            "data_hash": self.data_hash,
            "experiment_name": self.experiment_name
        }
