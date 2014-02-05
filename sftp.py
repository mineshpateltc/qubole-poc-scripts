#!/usr/bin/python
"""
This script will create an sftp connection, download files locally, then will copy the files to an s3 location.
The local files will be deleted upon copy to s3.

This is currently designed to work for the 49ers POC.

There will be a directory under the s3 path created for each 'category'. Under that, there will be a daily directory

So for example, the file: 49ersExportCustomers20140114001501.csv will be placed in <s3_path>/49ersExportCustomers/20140114/

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
parser.add_option('--trimheader', '--trimheader', default='true')
(opts, args) = parser.parse_args()

print 'INITIALIZING SFTP DOWNLOAD...'

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
print 'connecting to sftp using: ' + username + " / " + password
transport.connect(username = username, password = password)

sftp = paramiko.SFTPClient.from_transport(transport)

i = 0
while i <= daysback:
    filedate = datetime.date.today() - datetime.timedelta(i)
    datestr = filedate.strftime('%Y%m%d')

    print 'will get all files for: ' + datestr

    # get the files locally
    allfiles = sftp.listdir(sftppath)
    for datafile in allfiles:
        if datestr in datafile:
            print 'GETTING: ' + datafile
            # create local directory (ex: /media/ephemeral0/sftp_tmp/49ersExportCustomers/20140115/)
            localtargetdir = tmp_path + datafile[0:datafile.find(datestr)]+'/'+datestr+'/'
            if not os.path.exists(localtargetdir):
                print 'Creating local directory: '+localtargetdir
                subprocess.check_call(["mkdir", "-p", localtargetdir])
            sftp.get(sftppath+datafile, localtargetdir + datafile)

            # trim the first line of the file (in case it contains a header / column names)
            if opts.trimheader == 'true':
                print 'trimming the first line...'
                trimheader = ["sed", "-i", "1d", localtargetdir+datafile]
                subprocess.check_call(trimheader)

    i += 1

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
