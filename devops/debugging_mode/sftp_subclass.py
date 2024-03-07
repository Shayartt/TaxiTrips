import paramiko
import os


to_be_skipped_folders = ["doc", "__pycache__"]

#This subclass used to transfert a folder from local to remote server
class MySFTPClient(paramiko.SFTPClient):
    def put_dir(self, source, target):
        ''' Uploads the contents of the source directory to the target path. The
            target directory needs to exists. All subdirectories in source are 
            created under target.
        '''
        for item in os.listdir(source):
            if item in to_be_skipped_folders  : #to be ignored folders.
                continue
            elif os.path.isfile(os.path.join(source, item)):
                print(os.path.join(source, item), '%s/%s' % (target, item))
                self.put(os.path.join(source, item), '%s/%s' % (target, item))
                print(item)
            else:
                print(target, item)
                self.mkdir('%s/%s' % (target, item), ignore_existing=True)
                self.put_dir(os.path.join(source, item), '%s/%s' % (target, item))

    def mkdir(self, path, mode=511, ignore_existing=False):
        ''' Augments mkdir by adding an option to not fail if the folder exists  '''
        try:
            super(MySFTPClient, self).mkdir(path, mode)
        except IOError:
            if ignore_existing:
                pass
            else:
                raise