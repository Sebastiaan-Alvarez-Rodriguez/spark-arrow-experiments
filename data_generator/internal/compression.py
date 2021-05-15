from enum import Enum

class Compression(Enum):
    '''Known compression names.'''
    NONE = 0, 
    SNAPPY = 1, 
    GZIP = 2,
    BROTLI = 3,
    LZ4 = 4,
    ZSTD = 5

    @staticmethod
    def from_string(string):
        if string == None:
            return Compression.NONE
        return Compression[string.strip().upper()]

    def to_string(self):
        return self.name.lower()