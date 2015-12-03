# Module for Status of CMS6 via condor_q

import hf
from sqlalchemy import *
import json


class CacheSummary(hf.module.ModuleBase):
    config_keys = {'sourceurl': ('Source Url', '')
                   }
    table_columns = [
        Column('size', INT),
        Column('avail', INT),
        Column('used', INT),
        Column('score', INT),
        Column('error_msg', TEXT),
        Column('error', INT),
        Column('total_files', INT)
    ], []

    def prepareAcquisition(self):
        link = self.config['sourceurl']
        # Download the file
        self.source = hf.downloadService.addDownload(link)
        # Get URL
        self.source_url = self.source.getSourceUrl()
        # Set up Container for subtable data

    def extractData(self):
        data = {}
        data['error_msg'] = 0
        data['error'] = 0
        volume_size, volume_used, file_number, score_average, volume_avail = [], [], [], [], []
        path = self.source.getTmpPath()
        # Function to convert seconds to readable time format
        # open file
        with open(path, 'r') as f:
            # fix the JSON-File, so the file is valid
            content = f.read()
            content_fixed = content.replace("]", " ")
            content_fixed = content_fixed.replace("[", " ")
            services = json.loads(content_fixed)
            ekpsg = list(services.keys())
            for id in ekpsg:
                if services[id] != 'no data':
                    volume_size.append(int(services[id]['volume']['total']))
                    volume_avail.append(int(services[id]['volume']['avail']))
                    volume_used.append(int(services[id]['volume']['used']))
                    file_number.append(int(services[id]['allocation']['files_total']))
                    score_average.append(float(services[id]['allocation']['score_average']))
                else:
                    data['error'] += 1
        data['size'] = sum(volume_size)/(1024*1024*1024)
        data['avail'] = sum(volume_avail)/(1024*1024*1024)
        data['used'] = sum(volume_used)/(1024*1024*1024)
        if data['error'] < 4:
            try:
                data['score'] = round(sum(score_average)/len(score_average), 2)
            except ZeroDivisionError:
                data['score'] = 0
        else:
            data['error_msg'] = "No data to display!"
            data['status'] = 0
        data['total_files'] = sum(file_number)
        print data
        return data
