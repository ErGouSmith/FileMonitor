#!/usr/bin/env python
# -*- coding:utf-8 -*-
#import subprocess as sb
#import optparse as op
import os
import time,datetime
import FileInfor
import cx_Oracle as cx
import sys
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
dirOrFile = sys.argv[1]
#pdfDirLevel = sys.argv[2]
#DBTableInfor = []
#pdfDirs = []
#pdfFiles = []
#directorys = []
updateFiles = []
insertFiles = []
DBDirInfor = [] #the directories data already exist in DB
DBFileInfor = []#the files data already exist in DB
business_type = '03'
targetTable = 'file_monitor'
batch = 5000    #the num of inserts data each time
exceptDirs = ['/exam/rsync_logs']
def main():
    global DBFileInfor
    global DBDirInfor
    global targetTable
    global dirOrFile
    global insertFiles
    global updateFiles
    nowTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    startlog = '----------\nmonitor File\nStart at %s\ntargetDir is %s\n----------' %(nowTime,dirOrFile)
    #startlog = '----------\nStart monitor File at ' + nowTime + '\\ntargetDir is ' + dirOrFile + '\n----------'    #python3
    print startlog  #log
    conn = cx.connect('cdbp','cdbp',cx.makedsn('10.107.174.130','1521','racdg1'), encoding = "UTF-8", nencoding = "UTF-8")
    cur = conn.cursor()
    DBDirInfor = getDirsFromDB(targetTable, cur)
    fileMonitor(dirOrFile, cur)
    #print(DBDirInfor)
    #print(DBFileInfor)
    insertDB(insertFiles, cur, conn, targetTable)
    updateDB(updateFiles, cur, conn, targetTable)
    for i in updateFiles:
        print "update: " + i.fileTotalDir + str(i.fileMTime)
    cur.close()
    conn.close()
    nowTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print 'finish at %s' %nowTime

#def getModifyDir(dirOrFile, cur, DBDirInfor, DBFileInfor, fatherModifyFlag = False, fatherExistDirFlag = True):

def fileMonitor(dirOrFile, cur):
    global DBFileInfor
    global DBDirInfor
    global targetTable
    global updateFiles
    global insertFiles
    global business_type
    global exceptDirs
    aFile = FileInfor.FileInfor(dirOrFile)  #init class fileinfor object
    #print aFile.fileTotalDir + '' + str(aFile.fileMTime) + '' + str(aFile.isDir) + '' + str(aFile.ExistFile) + '' + str(aFile.ExistDir)
    if (aFile.isDir) and (ifNotInExcept(aFile, exceptDirs)):    #when afile is directory and not in exceptDirs
        (ifExist, position) = ifHasExistInDB(aFile, DBDirInfor) #see if aFile's infor has been in DB, and its location

        if ifExist: #when aFile's infor has been in DB
            if ifModified(aFile, DBDirInfor, position): #when aFile modifyTime has changed
                updateFiles.append(aFile)
                if aFile.ExistFile: #when aFile(is dir) has PDF file in it
                    appendFiles(targetTable, cur, aFile.fileTotalDir)
                    #print aFile.fileTotalDir + ' s1:is a modify dir with pdfFiles in it'
                    for a in os.listdir(dirOrFile): #handle all children files
                        child = dirOrFile + os.sep + a
                        fileMonitor(child, cur)

                else:   #when aFile(dir) doesn't have any pdf file
                    #print aFile.fileTotalDir + ' s2:is a modify dir without pdfFiles in it'
                    for a in os.listdir(dirOrFile):
                        child = dirOrFile + os.sep + a
                        if os.path.isdir(child):    #only handle childDir
                            fileMonitor(child, cur)

            else:       #when aFile modifyTime has not changed
                if aFile.ExistDir:
                    #print aFile.fileTotalDir + ' s3:is a exist dir with dirs in it'
                    for a in os.listdir(dirOrFile):
                        child = dirOrFile + os.sep + a
                        if os.path.isdir(child):
                            fileMonitor(child, cur)

        else:               #when aFile's infor not in DB
            insertFiles.append(getDBInsert(aFile))
            #print "insert : " + aFile.fileTotalDir + str(ifExist) + str(position)
            if aFile.ExistFile:
                #print aFile.fileTotalDir + ' s4:not exist dir with pdfFiles in it'
                appendFiles(targetTable, cur, aFile.fileTotalDir)
                for a in os.listdir(dirOrFile):
                    child = dirOrFile + os.sep + a
                    fileMonitor(child, cur)
            else:
                #print aFile.fileTotalDir + ' s5:not exist dir without pdfFiles in it'
                for a in os.listdir(dirOrFile):
                    child = dirOrFile + os.sep + a
                    if os.path.isdir(child):
                        fileMonitor(child, cur)

    elif (aFile.isFile) and (ifNotInExcept(aFile, exceptDirs)): #when aFile is file and not in except
        (ifExist, position) = ifHasExistInDB(aFile, DBFileInfor)
        if not ifExist:
            print "insert : " + aFile.fileTotalDir + str(ifExist) + str(position)
            insertFiles.append(getDBInsert(aFile))

    #else:
        #print(aFile.fileTotalDir , ' in exception directorys, not monitor target')

def ifNotInExcept(aFile, exceptDirs):
    for i in exceptDirs:
        if i != aFile.fileTotalDir:
            continue
        else:
            return False
    return True

def ifHasExistInDB(aFile, DBInfor):
    if aFile.isDir:
        for i in range(len(DBInfor)):
            DBtemp = DBInfor[i][1]
            if isSameString(aFile.fileTotalDir, DBtemp):
                return (True,i)
            else:
                continue
        return (False, -1)
    elif aFile.isFile:
        for i in range(len(DBInfor)):
            DBtemp = DBInfor[i][1]
            if isSameString(aFile.fileTotalDir, DBtemp):
                return (True,i)
            else:
                continue
        return (False, -1)
    else:
        return (False, -1)

def isSameString(s1, s2):
    l = len(s1)
    if l == len(s2):
        for i in range(l):
            if s1[-i] == s2[-i]:
                continue
            else:
                return False
        return True
    else:
        return False


def getAllDataFromDB(targetTable, cur):
    sql = "select file_name,file_directory,business_no,last_edit_time from " + targetTable + " where business_type = " + business_type
    cur.prepare(sql)
    result = cur.execute(None)
    return result.fetchall()

def getDirsFromDB(targetTable, cur):
    sql = "select file_name,file_directory,business_no,last_edit_time from " + targetTable + " where business_type = \'" +business_type+ "\' and type = 0"
    cur.prepare(sql)
    result = cur.execute(None)
    return result.fetchall()

def appendFiles(targetTable, cur, targetDir):   #to add list of files in DB, whose father dir is a new one or has changed its modifyTime
    global DBFileInfor
    sql = "select file_name,file_directory,business_no,last_edit_time from "  + targetTable + " where business_type = \'" + business_type + "\' and type = 1 and file_directory like \'" + targetDir+"%\'"
    cur.prepare(sql)
    result = cur.execute(None)
    temp = result.fetchall()
    for i in temp:
        DBFileInfor.append(i)

def ifModified(aFile, DBDirInfor, position):
    if aFile.fileMSeconds == int(time.mktime(DBDirInfor[position][3].timetuple())):
        return False
    else:
        log2 = "%s changed from %s to %s" %(aFile.fileTotalDir, aFile.fileMTime, DBDirInfor[position][3])
        print log2
        return True

def getOrgCode(aFile):
    global business_type
    if business_type == '03':
        if aFile.fileTotalDir == '/exam':
            return ''
        else:
            return aFile.fileTotalDir.split(os.sep)[2]
    else:
        return None

def getDBInsert(aFile):
    global business_type
    tempTuple = (aFile.fileName, aFile.fileType, aFile.fileTotalDir, aFile.fileMTime, aFile.fileCTime, getOrgCode(aFile), business_type, aFile.shortFileName, str(aFile.type), aFile.shortFileName, str(aFile.fileSize), aFile.dirTime)
    #print(tempTuple)
    return tempTuple

def updateDB(updateFiles, cur, conn, targetTable):
    global batch
    startTime = time.time()
    for i in updateFiles:
        print "update: " + i.fileTotalDir + " "+ str(i.fileMTime)
    dataset = list()
    count = 0
    sql = 'update '  + targetTable + ' set LAST_EDIT_TIME =  :1 where FILE_DIRECTORY = :2'
    try:
        for index, row in enumerate(updateFiles):
            dataset.append((row.fileMTime,row.fileTotalDir))
            print str(row.fileMTime) + row.fileTotalDir
            log1 = 'fileOrDir %s updated' %row[1]
            print log1
            count += 1
            if (index + 1) % batch == 0:
                cur.executemany(sql, dataset)
                conn.commit()
                dataset = []
                continue
    except Exception as e:
        print e
    finally:
        cur.executemany(sql, dataset)
        conn.commit()
        dataset = []

    elapsed = (time.time() - startTime)
    log5 = "update %d rows to Oracle in %d seconds" %(count, elapsed)
    print log5


def insertDB(insertFiles, cur, conn, targetTable):
    global batch
    startTime = time.time()
    dataset = list()
    count = 0
    sql = 'insert into '  + targetTable + '(FILE_NAME,EXTENSION,FILE_DIRECTORY,LAST_EDIT_TIME,FILE_CREATE_TIME,ORG_CODE,BUSINESS_TYPE,BUSINESS_NO,TYPE,SHORT_FILENAME,FILESIZE,REPORT_TIME) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)'
    try:
        for index, row in enumerate(insertFiles):
            dataset.append(row)
            log3 = 'fileOrDir %s inserted' %row[2]
            print log3
            count += 1
            if (index + 1) % batch == 0:
                cur.executemany(sql, dataset)
                conn.commit()
                dataset = []
                continue
    except Exception as e:
        print e
    finally:
        cur.executemany(sql, dataset)
        conn.commit()
        dataset = []

    elapsed = (time.time() - startTime)
    log4 = "insert %d rows to Oracle in %d seconds" %(count, elapsed)
    print log4

main()
