import time,datetime
import os
import math
class FileInfor:
    fileTotalDir = ''
    isFile = False
    isDir = False
    type = 0
    fileName = ''
    shortFileName = ''
    fileSize = 0
    fileDir = ''    #descript the file directory without file name in it
    fileType = ''   #direcotory or all kind of files,like pdf,txt etc
    fileMTime = datetime.datetime(1970,01,01,00,00,00)
    fileCTime = datetime.datetime(1970,01,01,00,00,00)
    fileATime = datetime.datetime(1970,01,01,00,00,00)
    fileMSeconds = 0
    fileCSeconds = 0
    fileASeconds = 0
    childDir = []
    ExistFile = False
    ExistDir = False
    isPDF = False
    dirTime = ''
    def __init__(self,fileTotalDir):
        self.fileTotalDir = fileTotalDir
        #set createTime,modifyTime,accessTime
        self.fileMSeconds = int(math.floor(os.path.getmtime(self.fileTotalDir)))
        self.fileCSeconds = int(math.floor(os.path.getctime(self.fileTotalDir)))
        self.fileASeconds = int(math.floor(os.path.getatime(self.fileTotalDir)))
        self.fileMTime = datetime.datetime.strptime(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(self.fileMSeconds)), "%Y-%m-%d %H:%M:%S")
        self.fileCTime = datetime.datetime.strptime(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(self.fileCSeconds)), "%Y-%m-%d %H:%M:%S")
        self.fileATime = datetime.datetime.strptime(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(self.fileASeconds)), "%Y-%m-%d %H:%M:%S")
        #set file direcotory and fileName(in this step, directory will have the same result like file)
        self.fileDir = os.path.split(self.fileTotalDir)[0]
        self.fileName = os.path.split(self.fileTotalDir)[1]
        if os.path.isdir(self.fileTotalDir):
            self.isDir = True
            self.fileType = 'directory'
            self.childDir = os.listdir(self.fileTotalDir)
            self.ExistFile = self.ifExistPDF(self.childDir)
            self.ExistDir = self.ifExistDir(self.childDir, self.fileTotalDir)
            self.shortFileName = self.fileName
            self.dirTime = self.fileCTime
            self.type = 0
        else:
            self.isFile = True
            self.fileSize = os.path.getsize(self.fileTotalDir)
            self.shortFileName = os.path.splitext(self.fileName)[0]
            self.fileType = os.path.splitext(self.fileTotalDir)[1].split(os.extsep)[1]
            self.isPDF = self.ifPDF(self.fileTotalDir)
            self.dirTime = datetime.datetime.strptime(self.fileTotalDir.split(os.sep)[3],'%Y%m%d')
            self.type = 1

    def ifExistDir(self, childDir, fileTotalDir):
        for i in childDir:
            if os.path.isdir(fileTotalDir + os.sep + i):
                return True
                break
            else:
                continue
        return False

    def ifExistPDF(self, childDir):
        for i in childDir:
            if os.path.splitext(i)[1] == '.pdf':
                return True
                break
            else:
                continue
        return False

    def ifPDF(self, fileTotalDir):
        if os.path.splitext(fileTotalDir)[1] == '.pdf':
            return True
        else:
            return False
