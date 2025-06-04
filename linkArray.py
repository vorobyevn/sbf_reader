# -*- coding: utf-8 -*-
import sys
sys.path.append('../../general')
import app

class linkArray():

    def __init__(self, maxLink):
        self.recLinks = {}
        self.maxLink = 0
        self.maxLink = maxLink

    def add(self, links, visite=False, isRecord=False):
        if hasattr(links, "__iter__"):
            for link in links:
                self.addEl(link, visite, isRecord)
        else:
            self.addEl(links, visite, isRecord)

    def addEl(self, link, visite=False, isRecord=False):
        if not isinstance(link, (int)):
            return
        if link in self.recLinks:
            self.recLinks[link]['visite'] = visite
            self.recLinks[link]['isRecord'] = isRecord
            return
        if link <= 0:
            return
        if link >= self.maxLink:
            app.printError('Error, link out of file {0}, max {1}'.format(hex(link), hex(self.maxLink)))
            return
        self.recLinks[link] = {'visite':visite, 'isRecord':isRecord}

    def linkExist(self, link):
        if link in self.recLinks:
            return True
        else:
            return False

    def linkIsRecord(self, link):
        if self.linkExist(link):
            return self.recLinks[link]['isRecord']
        else:
            return None

    def getNotVisitedLinks(self):
        list = [el for el in self.recLinks if self.recLinks[el]['visite'] == False]
        list = sorted(list)
        return list

    def getNotVisitedCount(self):
        return len(self.getNotVisitedLinks())

    def setVisited(self, link, isRecord):
        self.recLinks[link]['visite'] = True
        self.recLinks[link]['isRecord'] = isRecord

    def getRecordLinks(self):
        list = [el for el in self.recLinks if self.recLinks[el]['isRecord'] == True]
        return sorted(list)