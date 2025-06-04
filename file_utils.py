# -*- coding: utf-8 -*-
import codecs
import glob
import logging
import os
from os.path import basename
import shutil
import re
# import sys
# from xml.dom import minidom

def remove_file(path):
    os.remove(path)

def remove_files(path):
    for f in glob.glob(path):
        os.remove(f)

def copy_file(srcPath, dstFolder, newName):
    if not os.path.exists(srcPath):
        return
    # fileName = os.path.basename(srcPath)
    # fileName = fileName.split('.')[0]
    dstPath = dstFolder + newName
    shutil.copyfile(srcPath, dstPath)

def move_file(src_path, dst_path):
    file = get_full_filename(src_path)
    shutil.move(src_path, dst_path + file)

def get_first_file(path):
    fileList = glob.glob(path)
    if len(fileList) > 0:
        return fileList[0]
    else:
        return None

def get_files(path):
    return glob.glob(path)

def get_filename(path):
    base = os.path.basename(path)
    parts = os.path.splitext(base)
    if len(parts) > 0:
        return parts[0]
    return ''

def get_full_filename(path):
    return os.path.basename(path)

def getNewFile(name):
    fileList = glob.glob(name)
    newfile = ''
    maxNum = 0
    for path in fileList:
        file = os.path.basename(path)
        num = int(re.sub('[^0-9]', '', file))
        if num > maxNum or maxNum == 0:
            maxNum = num
            newfile = path
    return newfile

def readFile(file, codepage='utf-8'):
    with open(file, 'r', encoding=codepage) as f:
        data = f.read()
        # data = f.readlines()
        # data = ''.join(data).strip()
    return data