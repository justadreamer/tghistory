from pydrive2.auth import GoogleAuth
from pydrive2.auth import RefreshError
from pydrive2.drive import GoogleDrive
import os
from pathlib import PurePath

class Drive:
    def __init__(self):
        self.drive = GoogleDrive(self.auth)

    @property
    def auth(self):
        dir = os.path.dirname(os.path.realpath(__file__))
        credentialsFileName = os.path.join(dir,'credentials.json')
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile(credentialsFileName)

        if gauth.access_token_expired:
            try:
                gauth.Refresh()
            except RefreshError:
                gauth.CommandLineAuth()
                gauth.SaveCredentialsFile(credentialsFileName)

        return gauth

    def fileListFrom(self,root):
        q = "'" + root + "' in parents"
        fileList = self.drive.ListFile({'q': q}).GetList()
        return fileList

    def printFileList(self,fileList):
        for file in fileList:
            print("title:", file['title'], "id:", file['id'])

class File:
    def __init__(self,file):
        self.file = file

    @property
    def fileSize(self):
        return int(self.file['fileSize'])

    @property
    def title(self):
        return self.file['title']

    @property
    def id(self):
        return self.file['id']

    def delete(self):
        self.file.Delete()


class Folder:
    def __init__(self, path: PurePath, createIfNotExists=False):
        self.driveWrapper = Drive()
        self.path = path
        self.folder = self.folder(createIfNotExists=createIfNotExists)
        self.files = self.files()

    def folder(self, createIfNotExists=False):
        parent_id = 'root'
        folder = {'id': parent_id} #by default we are root folder
        for component in self.path.parts:
            fileList = self.driveWrapper.fileListFrom(parent_id)
            filtered = list(filter(lambda f: f['title']==component, fileList))
            if len(filtered)>0:
                folder = filtered[0]
            elif createIfNotExists:
                print(f"creating {component} folder")
                folder = self.createFolder(parent_id, component)
            parent_id = folder['id'] #for intermediate folders

        return folder

    def createFolder(self, parent_id, name):
        metadata = { 'parents': [ { "kind": "drive#fileLink", "id": parent_id } ], 'title': name, 'mimeType': 'application/vnd.google-apps.folder' }
        file = self.driveWrapper.drive.CreateFile(metadata=metadata)
        file.Upload()
        return file

    def files(self):
        fileList = self.driveWrapper.fileListFrom(self.folder['id'])
        return fileList

    def downloadAll(self,path):
        for f in self.files:
            filePath = os.path.join(path,f['title'])

            needsDownload = True
            if os.path.exists(filePath):
                upSize = int(f['fileSize'])
                downSize = os.stat(filePath).st_size
                if upSize == downSize:
                    print(f['title'] + " already downloaded")
                    needsDownload = False

            if needsDownload:
                print("Downloading "+f['title'])
                f.GetContentFile(filePath)

    def fileForName(self,name):
        for f in self.files:
            if f['title']==name:
                return File(f)
        return None

    def upload(self,path):
        fileName = os.path.basename(path)
        serverFile = self.fileForName(fileName)
        if serverFile is not None:
            if serverFile.fileSize == os.stat(path).st_size:
                print(serverFile.title + " already uploaded")
                return serverFile.file
            else:
                serverFile.delete() #we are going to replace this file

        metadata = { 'parents': [ { "kind": "drive#fileLink", "id": self.folder['id'] } ], 'title': fileName }
        file = self.driveWrapper.drive.CreateFile(metadata=metadata)
        file.Upload()
        file.SetContentFile(path)
        file.Upload()
        return file

