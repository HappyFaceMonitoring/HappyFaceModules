# -*- coding: utf-8 -*-
import hf, lxml, logging, datetime
from sqlalchemy import *
import json
from string import strip
import time

class CMSPhedexDataExtract(hf.module.ModuleBase):
    
    config_keys = {
        'link_direction': ("transfers 'from' or 'to' you", 'from'),
        'time_range': ('set timerange in hours', '72'),
        'base_url': ('use --no-check-certificate, end base url with starttime=', ''),
        'report_base':("insert base url for reports don't forget fromfilter or tofilter with your name!",'https://cmsweb.cern.ch/phedex/prod/Activity::ErrorInfo?tofilter=T1_DE_KIT&'),
        'blacklist': ('ignore links from or to those sites, csv', ''),
        'your_name': ('Name of you position', 'T1_DE_KIT_Buffer')
    }
    config_hint = 'If you have problems downloading your source file, use: "source_url = both|--no-check-certificate|url"'
    
    table_columns = [
        Column('direction', TEXT),
        Column('request_timestamp', INT),
        Column('time_range', INT),
    ], []
    
    subtable_columns = {
        'details': ([
            Column('done_files', INT),
            Column('fail_files', INT),
            Column('timebin', INT),
            Column('rate', INT),
            Column('name', TEXT)
        ], [])
    }
    
    def prepareAcquisition(self):
        self.url = self.config['base_url']
        self.link_direction = self.config['link_direction']
        self.your_name = self.config['your_name']
        if self.link_direction == 'from':
            self.parse_direction = 'to'
        else:
            self.parse_direction = 'from'
        self.time_range = int(self.config['time_range'])
        try: 
            self.blacklist = []
        except AttributeError:
            self.blacklist = None
        self.time = int(time.time())
        self.url += str(self.time-self.time_range*3600)
        self.source = hf.downloadService.addDownload(self.url)
        self.details_db_value_list = []
        
    def extractData(self):
        data = {'direction' : self.link_direction, 'source_url' : self.source.getSourceUrl(), 'time_range' : self.time_range, 'request_timestamp' : self.time}
        #prepare everything
        y = []
        x = []
        wei = []
        ykey = []
        x0 = self.time/3600*3600-72*3600
        height = 0
        i = 0
        xpos = []
        for i in range(self.time_range / 6):
            xpos.append(i * 6)
        xpos.append(self.time_range)
        xkey = []
        h0 = int(24*(self.time/3600.0/24.0 - self.time/3600/24)) + 6
        for i in enumerate(xpos):
            h0 -= 6
            if h0 < 0:
                h0 += 24
            xkey.append('%2i:00' %h0)
        xkey.reverse()
        # parse data
        fobj = json.load(open(self.source.getTmpPath(), 'r'))['phedex']['link']
        for i,links in enumerate(fobj):
            if links[self.link_direction] == self.your_name and links[self.parse_direction] not in self.blacklist:
                link_name = links[self.parse_direction]
                ykey.append(link_name)
                for j,transfer in enumerate(links['transfer']):
                    help_append = {}
                    x.append(int(transfer['timebin']-x0)/3600)
                    y.append(height)
                    help_append['timebin'] = int(transfer['timebin'])
                    help_append['done_files'] = done = int(transfer['done_files'])
                    help_append['fail_files'] = fail = int(transfer['fail_files'])
                    help_append['rate'] = int(transfer['rate'])
                    help_append['name'] = link_name
                    self.details_db_value_list.append(help_append)
                    if done != 0:
                        wei.append(float(done)/float(done + fail))
                    elif done == 0 and fail != 0:
                        wei.append(0.0)
                    elif done == 0 and fail == 0:
                        wei.append(1.5)
                height += 1
        if len(y) > 0: 
            H = []
            for i in range(0,height):
                H.append([])
                for j in range(0,self.time_range + 1):
                    H[i].append(1.5)
            for i,group in enumerate(x):
                H[y[i]][group] = wei[i]
            T1_data = []
            T2_data = []
            for i,name  in enumerate(ykey):
                if 'T1' in name:
                    T1_data.append(H[i])
                elif 'T2' in name:
                    T2_data.append(H[i])
            
            for i in range(3):
                bad1 = 0
                sum1 = 0
                sum2 = 0
                bad2 = 0
                for j, link in enumerate(T1_data):
                    if  link[self.time_range - j] <= 0.5:
                        bad1 += 1
                        sum1 += 1
                    elif link[self.time_range - j] != 1.5:
                        sum1 += 1
                for j, link in enumerate(T2_data):
                    if link[self.time_range - j] <+ 0.5:
                        bad2 += 1
                        sum2 += 1
                    elif link[self.time_range - j] != 1.5:
                        sum2 += 1

            data['status'] = 1.0
            try:
                if float(bad1) / sum1 >= 0.5 or float(bad2) / sum2 >= 0.5:
                    data['status'] = 0.0
            except ZeroDivisionError:
                if sum1 == 0 and sum2 != 0:
                    if float(bad2) / sum2 >= 0.5:
                        data['status'] = 0.0
                elif sum1 != 0 and sum2 == 0:
                    if float(bad1) / sum1 >= 0.5:
                        data['status'] = 0.0
        else:
            data['status'] = -1
            data['error_string'] = 'No data to parse!'
        return data
    
    #def plot_func(self, H, xbin, ybin, y_key, x_pos, x_key):
        #import matplotlib.colors as mcol
        #import matplotlib.pyplot as plt
        #import numpy as np
        #if ybin < 8:
            #ysize = 8
        #else:
            #ysize = ybin
        #fig = plt.figure(figsize=(16,ysize))
        #H = []
        #for i in range(0,ybin):
            #H.append([])
            #for j in range(0,xbin):
                #H[i].append(0.0)
        #for i,group in enumerate(x):
            #H[y[i]][group] = w[i]
        #H = np.array(H)
        #extent = [0, xbin, 0, ybin]
        #my_cmap = plt.get_cmap('RdYlGn')
        #my_cmap.set_over('w')
        #norm = mcol.Normalize(0.0, 1)
        #plt.imshow(H, extent = extent, interpolation = 'nearest', cmap = my_cmap, norm=norm, aspect='auto', rasterized=True, origin = 'lower')
        #plt.colorbar(orientation = 'horizontal', fraction = 0.15, shrink = 0.8)
        #plt.grid(linewidth=1.2, which = 'both')
        #plt.yticks(np.arange(0, ybin) + 0.5, y_key, size = 'medium')
        #plt.xticks(x_pos, x_key, size = 'medium', rotation = 30)
        #fig.savefig(hf.downloadService.getArchivePath(self.run, self.instance_name + '_phedex_' + self.link_direction + self.your_name + '_quality.png'), dpi=60)
        #return(self.instance_name + '_phedex_' + self.link_direction + self.your_name + '_quality.png')
    

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])
    def getTemplateData(self):
        report_base = strip(self.config['report_base'])
        your_direction = strip(self.config['link_direction'])
        if your_direction == 'from':
            their_direction = 'tofilter'
            your_direction = 'fromfilter'
        else:
            their_direction = 'fromfilter'
            your_direction = 'tofilter'
            
        import matplotlib.colors as mcol
        import matplotlib.pyplot as plt
        import numpy as np
        #define colormap
        my_cmap = plt.get_cmap('RdYlGn')
        my_cmap.set_over('w')
        #get data from database
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).execute().fetchall()
        #build a 2d histogram and fill it with transfer quality
        H = []
        x = []
        y = []
        w = []
        fails = []
        done = []
        rates = []
        times = []
        x0 = self.dataset['request_timestamp'] / 3600 * 3600 - 259200
        y_value_map = {}
        for i,values in enumerate(details_list):
            if values['name'] not in y_value_map:
                y_value_map[values['name']] = len(y_value_map)
            x.append(int(values['timebin']-x0)/3600)
            times.append(datetime.datetime.fromtimestamp(values['timebin']))
            y.append(y_value_map[values['name']])
            fails.append(int(values['fail_files']))
            done.append(int(values['done_files']))
            rates.append(float(values['rate'])/1024/1024)
            if values['done_files'] != 0:
                w.append(float(values['done_files'])/float(values['done_files'] + values['fail_files']))
            elif values['done_files'] == 0 and values['fail_files'] != 0:
                w.append(0.0)
            elif values['done_files'] == 0 and values['fail_files'] == 0:
                w.append(1.5)
        H = []
        name_mapper = []
        for i in range(len(y_value_map)):
            H.append([])
            name_mapper.append('-')
            for j in range(self.dataset['time_range'] + 1):
                    H[i].append({'w':'---', 'c':'#FFFFFF', 'fails': '---', 'done':'---', 'rates':'---', 'time':'---', 'link': '---'})
        for key in y_value_map.iterkeys():
            name_mapper[y_value_map[key]] = key
        for i,group in enumerate(x):
            if w[i] != 1.5:
                H[y[i]][group] = {'w':str('%.2f' %w[i]), 'fails':fails[i], 'done':done[i], 'rates':str('%.3f' %rates[i]), 'c':mcol.rgb2hex(my_cmap(w[i])), 'time':times[i], 'link':report_base + their_direction + name_mapper[y[i]]}
            else:
                H[y[i]][group] = {'w':'---', 'c':'#FFFFFF', 'fails': '---', 'done':'---', 'rates':'---', 'time':'---', 'link': '---'}
        #iterate over histogram H and fill each bin with an dictionary
        data['histogram'] = H
        data['names'] = name_mapper
        return data