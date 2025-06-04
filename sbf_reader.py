# -*- coding: utf-8 -*-
import os
import sys
sys.path.append('../../general')
import app
import file_utils
from dal_log import dalLog
from db import dal
from linkArray import linkArray
from buffer import buffer
from objects import LogEvent
import mmap
import struct
import traceback

class SbfReader():
    def __init__(self, filename, tablename=''):
        self.tablename = ''
        self.mmFile = None
        self.fileMaxOffset = 0
        self.fields = []
        self.fieldsCount = 0
        self.recCount = 0
        self.recLinks = None
        self.isDic = False
        self.merge_field = 'merge_stat'
        self.id_field_key = 2
        self.filename = filename
        if tablename == '':
            self.tablename = self.getTableNameByFilename(self.filename)
        else:
            self.tablename = tablename

    def readSbf(self):
        self.isDic = self.tablename[:3] == 'dic'
        self.fileMaxOffset = self.getFileMaxOffset(self.filename)
        self.recLinks = linkArray(self.fileMaxOffset)
        with open(self.filename, "r+b") as f:
            self.mmFile = mmap.mmap(f.fileno(), 0)
            app.printInfo('Clear table %s'%self.tablename)
            dal.clearTable(self.tablename)
            self.processFile()
            self.mmFile.close()
        self.fields = None
        return self.recCount

    def processFile(self):
        self.fieldsCount, self.fields = self.getFieldsData()
        self.printFields(self.fields)
        # ev = LogEvent('parser', 'cian')
        # ev.info = {'file': 'filename.html'}
        # dalLog.saveLogEvent()
        self.recLinks.add(self.getDataBlockOffset())
        self.searchLinks()
        recLinks = self.recLinks.getRecordLinks()
        self.recCount = len(recLinks)
        app.printInfo('Found %s records'%len(recLinks))
        bulk = []
        idx = 0
        for i in range(0, self.recCount):
            recLink = recLinks[i]
            if i < len(recLinks)-1:
                nextLink = recLinks[i+1]
            else:
                nextLink = self.fileMaxOffset + 1
            try:
                row, id_name = self.readFields(recLink, nextLink-recLink)
                if row is not None:
                    idx += 1
                    bulk.append(row)
                if idx % 1000 == 0 or i == self.recCount - 1:
                    dal.insertRecBulk(self.tablename, bulk, id_name)
                    print('Add %s'%idx)
                    bulk = []
            except Exception as ex:
                app.printError('Error in record parsing')
                app.printError(ex)
                app.printError(traceback.format_exc())
        app.printInfo('Loaded %s'%(idx+1))

    def getFieldsData(self):
        part1Array = self.readDataByStruct('>4sl7b')
        part2Offset = part1Array[1]
        recOffset = self.mmFile.tell()
        recLng = part2Offset - recOffset
        fieldsCount, fields = self.readRecord(recOffset, recLng)
        return fieldsCount, fields

    def getDataBlockOffset(self):
        dataBlockOffset = self.readPreDataBlock1()
        if dataBlockOffset == 0:
            raise Exception('Error, offset not found {0}'.format(hex(dataBlockOffset)))
        dataBlockOffset += 1
        return dataBlockOffset

    def processRow(self, rec):
        row = {}
        for idx in rec:
            field = rec[idx]
            row[field['name']] = field['data']
        if self.isDic and "facet_id" in row:
            row.pop("facet_id")
        else:
            row[self.merge_field] = 's'
        return row

    def searchLinks(self):
        while self.recLinks.getNotVisitedCount() > 0:
            for link in self.recLinks.getNotVisitedLinks():
                self.blockParse(link)

    def blockParse(self, dataBlockOffset):
        app.printDebug(hex(dataBlockOffset) + ' - parse block')
        self.recLinks.setVisited(dataBlockOffset, isRecord=False)
        if self.dataIsRecords(dataBlockOffset):
            self.recLinks.setVisited(dataBlockOffset, isRecord=True)
        self.seek(dataBlockOffset)
        data = self.readDataByStruct('>256I')
        for idx in range(0,len(data)):
            intgr = data[idx]
            intgrIsRec = False
            if intgr == 0:
                continue
            is_rec = self.dataIsRecords(intgr)
            if is_rec is None:
                continue
            if is_rec:
                app.printDebug('{0} is record'.format(hex(intgr)))
                self.recLinks.add(data[idx:], visite=True, isRecord=True)
                break
            else:
                self.recLinks.add(intgr, visite=False)

    def dataIsRecords(self, link):
        if link >= self.fileMaxOffset:
            return None
        self.seek(link)
        if self.recLinks.linkExist(link):
            return self.recLinks.linkIsRecord(link)
        app.printDebug(hex(link) + ' - check is record')
        data = self.readDataByStruct('>4b')
        if data[0] > 0 and data[0] < 0x1F:
            fDataType = (data[2] & 0xF0) >> 4
            if data[0] <= self.fieldsCount and data[1] == 2 and fDataType == 4:
                return True
            else:
                return False
        elif data[0] == 0x1F:
            p1Count = (data[0] & 0xF0) >> 4
            count = data[1] + p1Count
            fDataType = (data[3] & 0xF0) >> 4
            if count <= self.fieldsCount and data[2] == 2 and fDataType == 4:
                return True
            else:
                return False
        return False

    def readPreDataBlock1(self):
        dataBlockOffset = self.readDataByStruct('>BQ')[1]
        return dataBlockOffset

    def readPreDataBlock2(self):
        dataBlockOffsets = self.readDataByStruct('>256I')
        return dataBlockOffsets

    def readFields(self, recOffset, recLng):
        id_name = ''
        if recOffset == 0 or recLng == 0:
            return (None, id_name)
        self.seek(recOffset)
        _, recFields = self.readRecord(recOffset, recLng)
        recFields = self.prepareFields(self.fields, recFields)
        if recFields is None:
            app.printInfo('Id is NULL, offset %s' % recOffset)
            return (None, id_name)
        id_name = recFields[self.id_field_key]['name']
        row = self.processRow(recFields)
        if row[id_name] is None:
            return None, id_name
        return row, id_name

    def readRecord(self, recOffset, recLng):
        recFields = []
        buf = buffer(self.mmFile.read(recLng), recOffset)
        count = self.readFieldsCount(buf)
        fieldDescrs = buf.readDataByStruct('>{0}H'.format(count))
        for fieldDescr in fieldDescrs:
            fIndex = (fieldDescr & 0xFF00) >> 8
            fType = (fieldDescr & 0x00FF)
            if fType == 0:
                continue
            byteCount = (fType & 0x0F)
            recField = {}
            recField['index'] = fIndex
            app.printDebug('Offset {0}'.format(hex(buf.tell())))
            recField['type'] = self.readRecordType(fType)
            try:
                recField['data'] = self.readFieldData(buf, byteCount)
            except Exception as ex:
                app.printError("Read data error. Index %s, offset %s, type %s, bytes %s"%(recField['index'], hex(buf.tell()), recField['type'], byteCount))
                app.printError(ex)
                recField['data'] = None
            if recField['index'] == self.id_field_key and recField['data'] is None:
                return 0, []
            recFields.append(recField)
        for recField in recFields:
            if recField['type'] == 's':
                recField['data'] = self.readStringFieldData(buf, recField['data'])
        buf = None
        return count, recFields

    def readFieldsCount(self, buf):
        filePos = buf.tell()
        part1 = buf.readDataByStruct('>b')[0]
        if part1 < 0x1F:
            count = part1
        elif part1 == 0x1F:
            p1Count = (part1 & 0xF0) >> 4
            part2 = buf.readDataByStruct('>b')[0]
            count = part2 + p1Count
        else:
            raise Exception('Error, unknown fields count {0}, offset={1}'.format(hex(part1), hex(filePos)))
        return count

    def readRecordType(self, fType):
        fDataType = (fType & 0xF0) >> 4
        if fDataType == 8:
            return 's'
##        elif fType == int(0x44):
##            return 'd'
        elif fDataType == 4:
            return 'i'
        else:
            raise Exception("Error, unknown field type={0}".format(fType))

    def readFieldData(self, buf, byteCount):
        data = 0
        if byteCount == 0:
            data = 0 #None
        elif byteCount == 1:
            data = buf.readDataByStruct('B')[0]
        elif byteCount == 2:
            data = buf.readDataByStruct('>H')[0]
        elif byteCount == 3:
            arr = buf.readDataByStruct('>3B')
            data = self.intFromBytes(0, arr[0], arr[1], arr[2])
        elif byteCount == 4:
            data = buf.readDataByStruct('>l')[0]
        else:
            raise Exception('Error: Unknown byteCount {0}'.format(byteCount))
        return data

    def readStringFieldData(self, buf, lenght):
        if lenght != None:
            strStruct = '>{0}s'.format(lenght)
            app.printDebug('Struct {0}'.format(strStruct))
            dataStr = buf.readDataByStruct(strStruct)[0]
            return dataStr.decode("windows-1251").replace("'", "")
        else:
            return ''

    def prepareFields(self, fieldsDef, recFields):
        if len(recFields) == 0:
            return None
        dbFields = {}
        for fldDef in fieldsDef:
            fld = {}
            fld['name'] = fldDef['data']
            fld['data'] = None
            dbFields[fldDef['index']] = fld
        for recField in recFields:
            recFieldIdx = recField['index']
            dbFields[recFieldIdx]['type'] = recField['type']
            dbFields[recFieldIdx]['data'] = recField['data']
        return dbFields

    def printFields(self, fields):
        app.printDebug([f['data'] for f in fields])

    def seek(self, offset):
        app.printDebug('Seek {0}'.format(hex(offset)))
        try:
            self.mmFile.seek(offset)
        except Exception as ex:
            pass

    def readDataByStruct(self, structTempl):
        app.printDebug('Struct {0}'.format(structTempl))
        count = struct.calcsize(structTempl)
        app.printDebug('Read {0} bytes'.format(hex(count)))
        buf = self.mmFile.read(count)
        return struct.unpack(structTempl, buf)

    def intFromBytes(self, b3, b2, b1, b0):
        return (((((b3 << 8) + b2) << 8) + b1) << 8) + b0

    def getTableNameByFilename(self, path):
        filename = file_utils.get_filename(path)
        filename = filename[:filename.find('.')]
        return filename.replace('-', '_')

    def getFileMaxOffset(self, path):
        return os.path.getsize(path) - 1
