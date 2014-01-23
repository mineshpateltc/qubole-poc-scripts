import optparse
import datetime

parser = optparse.OptionParser()

parser.add_option('--host', '--host', default='foobar')
parser.add_option('--daysback', '--daysback', default=1, type="int")
(opts, args) = parser.parse_args()

print opts.host

daysback = opts.daysback


#for i in range (0,daysback):
i = 0
while i <= daysback:
    filedate = datetime.date.today() - datetime.timedelta(i)
    datestr = filedate.strftime('%Y%m%d')
    print datestr
    i += 1




trimheader = ["sed", "-i", "-e", """ "1d" """]
print(trimheader)