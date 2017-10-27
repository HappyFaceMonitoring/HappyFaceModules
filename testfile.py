# Import section
# this is only used in this file
import urllib as ul
# regular expressions used in parsing the strings
import re

#Download file from the web
url = "http://cmsdcacheweb-kit.gridka.de:2288/context/transfers.txt"
response = ul.urlopen(url)
data = response.read()
text = data.decode('utf-8')

# define regular expressions to match the different parts of the line
# one regular expression can hold multiple entries in it so that every entry is only matched once per line
Door_RE = re.compile(r'[a-zA-Z]+-f\d{2}-\d{3}-\d{3}-e(?:-(?:<\w+>-)?(:?\w+)+)?')
Domain_idRE = re.compile(r'(?P<domain>[a-z]+-f\d{2}-\d{3}-\d{3}-eDomain)\s(?P<Id>\d+)')
Gftp_data_RE = re.compile(r'(?P<proto>GFtp-\d)\s(?P<UID>\d{4,6})\s(?P<Proc>\d+)')
dcap_data_RE = re.compile(r'(?P<proto>dcap-\d)\s{2}(?P<Proc>\d+)', re.U)
xrootd_data_RE = re.compile(r'(?P<proto>Xrootd-\d\.\d)\s(?P<UID>\d+)')
PnfsID_RE = re.compile(r'[0-9A-F]{36}')
Host_RE = re.compile(r'(?:(?:25[0-5]|2[0-4][0-9]|[1]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[1]?[0-9][0-9]?)$')
Status_Wait_RE = re.compile(r'(?P<Status>WaitingForDoorTransferOk|(?:Mover\sf\d{2}-\d{3}-\d{3}-e_[a-zA-Z]{1,2}_[d]?cms/\d{8,9}:\s(?:Sending|Receiving)))\s(?P<Wait>\d+)')
State_time_up_RE = re.compile(r'(?P<State>RUNNING|No-mover\(\)-Found)\s?(?P<Time_up>\d+)?')
Transferd_Speed_MoverID_RE = re.compile(r'(?P<Transfered>\d+)\s(?P<Speed>\d+\.\d+)\s(?P<MoverID>\d+)$')

# Names of the collumns in the scource textfile
keys = ['Door', 'Domain', 'Id', 'Prot', 'UID', 'GID', 'VOMS Group', 'Proc', 'PnfsId', 'Pool', 'Host',
        'Status', 'Waiting', 'State', 'Transferrate (kB)', 'Speed (kB/s)', 'Mover_ID']


def get_protocoll_data_from_line(line):
    """Function checks which protocol was used and fills the table entries accordingly"""
    #check which protocoll was used by checking the return value of the regexes
    proto_data = xrootd_data_RE.search(line)
    proto = 'xroot'
    if proto_data is None:
        proto_data = dcap_data_RE.search(line)
        proto = 'dcap'
        if proto_data is None:
            proto_data = Gftp_data_RE.search(line)
            proto = 'gftp'
            if proto_data is None:
                raise Exception("Parsing of file failed")
    # fill the data
    proto_entry = proto_data.group('proto')
    uid_entry = None
    proc_entry = None
    if proto == 'gftp' or proto == 'xroot':
        uid_entry = proto_data.group('UID')
    if proto == 'dcap' or proto == 'gftp':
        proc_entry = proto_data.group('Proc')
    return proto_entry, uid_entry, proc_entry


# the table of the rearranged data
l = 0
table = []
protocolls = []
uids = []
proc = []
for line in data.split('\n'):
    l += 1
    if len(line) > 1:
        door_entry = Door_RE.search(line)
        domain_id_entry = Domain_idRE.search(line)
        #print domain_id_entry.group('Id')
        proto_entry, uid_entry, proc_entry = get_protocoll_data_from_line(line)
        if proto_entry:
            protocolls.append(proto_entry)
            uids.append(uid_entry)
            proc.append(proc_entry)

    # check which kind of protocoll was used and fill the protocoll related fields accordingly
protocolls = set(protocolls)
uids = set(uids)
proc = set(proc)
print len(protocolls), len(proc), len(uids)
print protocolls, proc, uids
