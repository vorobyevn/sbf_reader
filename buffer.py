# -*- coding: utf-8 -*-
import struct

class buffer():
    index = 0
    fileOffset = 0
    array = []

    def __init__(self, array, fileOffset = 0, startIndex = 0):
        self.index = startIndex
        self.fileOffset = fileOffset
        self.array = array

    def read(self, readLen):
        fi = self.index + readLen
        if fi > len(self.array):
            raise Exception("Read buffer error, read before={0}, max length={1}".format(fi, len(self.array)))
        res = self.array[self.index:fi]
        self.index += readLen
        return res

    def readByte(self):
        if self.index >= len(self.array):
            raise Exception("Read buffer error, index={0}, len={1}".format(self.index, len(self.array)))
        res = self.array[self.index]
        self.index += 1
        return res

    def readDataByStruct(self, structTempl):
        count = struct.calcsize(structTempl)
        buf = self.read(count)
        return struct.unpack(structTempl, buf)

    def tell(self):
        return self.fileOffset + self.index

    def eof(self):
        return self.index + 1 == len(self.array)