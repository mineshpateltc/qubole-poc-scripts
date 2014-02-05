#!/usr/bin/python
"""
This script will create an sftp connection, download files locally, then will copy the files to an s3 location.
The local files will be deleted upon copy to s3.

There will be a directory under the s3 path created for each file date.

inputs:
-host: host name of the sftp server (default: foo.bar.com)
-port: port of sftp server (default: 22)
-username
-password
-sftppath: sftp directory path
-tmp_path: temporarly local location
-s3_path: path of s3 location to copy files
-daysback: number of days back to download from sftp

"""
import paramiko
import time
import datetime
import os
import subprocess
import optparse


parser = optparse.OptionParser()
parser.add_option('--host', '--host', default='foo.bar.com')
parser.add_option('--port', '--port', default=22)
parser.add_option('--username', '--username', default='me')
parser.add_option('--password', '--password', default='')
parser.add_option('--sftppath', '--sftppath', default='./')
parser.add_option('--tmp_path', '--tmp_path', default='/media/ephemeral0/sftp_tmp/')
parser.add_option('--s3_path', '--s3_path', default='s3://mpatel.qubole.com/sftp_data_files/')
parser.add_option('--daysback', '--daysback', default=0, type=int)
(opts, args) = parser.parse_args()

print 'INITIALIZING SFTP DOWNLOAD...'
print 'Connecting to: ' + opts.host

host = opts.host
port = opts.port

username = opts.username
password = opts.password

sftppath = opts.sftppath
tmp_path = opts.tmp_path
s3_path = opts.s3_path

#daysback = 0
daysback = opts.daysback


"""
First Download the files using sftp.
"""

transport = paramiko.Transport((host, port))
#print 'connecting to sftp using: ' + username + " / " + password
transport.connect(username = username, password = password)

sftp = paramiko.SFTPClient.from_transport(transport)

startdate = datetime.date.today() - datetime.timedelta(daysback)
startdatestr = startdate.strftime('%Y%m%d')+'000000'
epochstartdate = int(time.mktime(time.strptime(startdatestr, '%Y%m%d%H%M%S')))

print 'attempting to download files from sftp server: ' + host
print 'all files newer than: ' + startdatestr + ' will be downloaded and transfered to: ' + s3_path

# get the files locally
allfiles = sftp.listdir_attr(sftppath)
for datafile in allfiles:
    # 'st_mtime' is a stat attribute referring to the last modified time
    epochfiledate = datafile.__getattribute__('st_mtime')

    if epochfiledate >= epochstartdate:
        filedatestr = datetime.datetime.fromtimestamp(epochfiledate).strftime('%Y%m%d')
        # create the local directory if needed
        if not os.path.exists(tmp_path+filedatestr+'/'):
             print 'Creating local directory: '+tmp_path+filedatestr+'/'
             subprocess.check_call(["mkdir", "-p", tmp_path+filedatestr+'/'])
        print 'Downloading: ' + datafile.filename
        print 'date of file: ' + filedatestr
        sftp.get(sftppath+datafile.filename, tmp_path+filedatestr+'/'+datafile.filename)

        # Unzip the files:
        unzip = ["unzip", "-o", tmp_path+filedatestr+'/'+datafile.filename, "-d", tmp_path+filedatestr+'/']
        subprocess.check_call(unzip)
        rmzip = ["rm", tmp_path+filedatestr+'/'+datafile.filename]
        subprocess.check_call(rmzip)

sftp.close()
transport.close()

"""
After downloading from SFTP location to a tmp_location locally,
we need to copy the files over to s3
"""

s3put = ["/usr/lib/s3cmd/s3cmd", "-c", "./s3cfg", "sync", tmp_path, s3_path]
print (s3put)
subprocess.check_call(s3put)


"""
We now need to clean up our mess
"""

print 'about to rm -r ' + tmp_path
subprocess.check_call(["rm", "-r", tmp_path])

print 'SFTP and File Transfer to S3 complete.'