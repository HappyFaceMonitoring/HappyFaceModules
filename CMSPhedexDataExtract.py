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
        'blacklist': ('ignore links from or to those sites, csv', ''),
        'your_name': ('Name of you position', 'T1_DE_KIT_Buffer')
    }
    config_hint = 'If you have problems downloading your source file, use: "source_url = both|--no-check-certificate|url"'
    
    table_columns = [
        Column('direction', TEXT),
        Column('request_timestamp', INT),
        Column('time_range', INT),
        Column('archive_pic_path', TEXT)
    ], ['archive_pic_path']
    
    subtable_columns = {
        'details': ([
            Column('done_files', INT),
            Column('fail_files', INT),
            Column('timebin', INT),
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
                    help_append['name'] = link_name
                    self.details_db_value_list.append(help_append)
                    if done != 0 and fail != 0:
                        wei.append(float(done)/float(done + fail))
                    elif done == 0 and fail != 0:
                        wei.append(0.002)
                    else:
                        try:
                            wei.append(float(done)/float(done + fail))
                        except ZeroDivisionError:
                            wei.append(0.0)
                height += 1
        
        data['archive_pic_path'] = self.plot_func(x,y,wei,self.time_range+1,height,ykey, xpos, xkey)
        data['status'] = 1.0
        return data
    
    def plot_func(self, x, y, w, xbin, ybin, y_key, x_pos, x_key):
        import matplotlib.colors as mcol
        import matplotlib.pyplot as plt
        import numpy as np
        if ybin < 8:
            ysize = 8
        else:
            ysize = ybin
        fig = plt.figure(figsize=(16,ysize))
        H = []
        for i in range(0,ybin):
            H.append([])
            for j in range(0,xbin):
                H[i].append(0.0)
        for i,group in enumerate(x):
            H[y[i]][group] = w[i]
        H = np.array(H)
        extent = [0, xbin, 0, ybin]
        my_cmap = plt.get_cmap('RdYlGn')
        my_cmap.set_under('w')
        norm = mcol.Normalize(0.001, 1)
        plt.imshow(H, extent = extent, interpolation = 'nearest', cmap = my_cmap, norm=norm, aspect='auto', rasterized=True, origin = 'lower')
        plt.colorbar(orientation = 'horizontal', fraction = 0.15, shrink = 0.8)
        plt.grid(linewidth=1.2, which = 'both')
        plt.yticks(np.arange(0, ybin) + 0.5, y_key, size = 'medium')
        plt.xticks(x_pos, x_key, size = 'medium', rotation = 30)
        fig.savefig(hf.downloadService.getArchivePath(self.run, self.instance_name + '_phedex_' + self.link_direction + self.your_name + '.png'), dpi=60)
        return(self.instance_name + '_phedex_' + self.link_direction + self.your_name + '.png')
        
    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])