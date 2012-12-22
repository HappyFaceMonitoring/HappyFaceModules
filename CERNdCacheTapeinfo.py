import hf, logging
from sqlalchemy import *
from datetime import datetime
#from lxml import etree

class CERNdCacheTapeinfo(hf.module.ModuleBase):
    config_keys = {
        'used_tape': ('URL of the used tape input txt file', ''),
        'total_tape': ('URL of the total tape input txt  file', ''),
        'used_disk': ('URL of the used disk input txt file', ''),
        'total_disk': ('URL of the total disk input txt file', ''),
    }
    config_hint = ''
    
    table_columns = [
        Column('used_tape_size', FLOAT),
        Column('used_tape_timestamp', INT),
        Column('total_tape_size', FLOAT),
        Column('used_disk_size', FLOAT),
        Column('total_disk_size', FLOAT),
    ], []


    def prepareAcquisition(self):

        if 'used_tape' not in self.config: raise hf.exceptions.ConfigError('used_tape option not set')
        self.used_tape = hf.downloadService.addDownload(self.config['used_tape'])

        if 'total_tape' not in self.config: raise hf.exceptions.ConfigError('total_tape option not set')
        self.total_tape = hf.downloadService.addDownload(self.config['total_tape'])

        if 'used_disk' not in self.config: raise hf.exceptions.ConfigError('used_disk option not set')
        self.used_disk = hf.downloadService.addDownload(self.config['used_disk'])

        if 'total_disk' not in self.config: raise hf.exceptions.ConfigError('total_disk option not set')
        self.total_disk = hf.downloadService.addDownload(self.config['total_disk'])


    
    def extractData(self):

        
        data = {'source_url': self.total_tape.getSourceUrl(),
                'used_tape_size': 0.0,
                'used_tape_timestamp': 0,
                'total_tape_size': 0.0,
                'used_disk_size': 0.0,
                'total_disk_size': 0.0,
                'status': 1.0}

        used_tape_f=-99.0
        for line in open(self.used_tape.getTmpPath()).readlines():
            if "T1_DE_KIT" not in line:
                continue
            used_tape_f=float(line.split()[3])
        data['used_tape_size'] = used_tape_f

     
        total_tape_f=-99.0
        used_tape_time=0
        all_lines=open(self.total_tape.getTmpPath()).read()
        for line in open(self.total_tape.getTmpPath()).readlines():
            if "<td>cms </td><td>" not in line:
                continue
            total_tape_f=round(float(line.split()[2]),3)
            #used_tape_time=datetime.strptime(line.split()[0]+" "+line.split()[1],'%Y-%m-%d %H:%M:%S')
       
        end_str= all_lines.find("Update  once per day at midnight")
        start_str = all_lines.find("Data on Grid SEs as of")

        date_string=all_lines[start_str:end_str].splitlines()[2]
      #  used_tape_time=datetime.strptime(date_string,'%a %b %d %H:%M:%S %Z %Y')


        data['total_tape_size'] = total_tape_f
        data['used_tape_timestamp'] = used_tape_time


            
        used_disk_f=-99.0
        for line in open(self.used_disk.getTmpPath()).readlines():
            if "T1_DE_KIT" not in line:
                continue
            used_disk_f=float(line.split()[3])
        data['used_disk_size'] = used_disk_f

        total_disk_f=-99.0
        for line in open(self.total_disk.getTmpPath()).readlines():
            if "T1_DE_KIT" not in line:
                continue
            total_disk_f=float(line.split()[3])
        data['total_disk_size'] = total_disk_f

        return data


    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        return data


