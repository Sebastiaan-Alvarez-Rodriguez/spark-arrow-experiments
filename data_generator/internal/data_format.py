from enum import Enum

class DataFormat(Enum):
    '''Known data formats.'''
    PARQUET = 0,
    CSV = 1

    @staticmethod
    def from_string(string):
        return DataFormat[string.strip().upper()]

    def to_string(self):
        return self.name().lower()