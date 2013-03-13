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
            Column('name', TEXT),
            Column('color', TEXT),
            Column('quality', FLOAT)
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
            self.blacklist = self.config['blacklist']
        except AttributeError:
            self.blacklist = []
        self.time = int(time.time())/3600*3600
        self.url += str(self.time-self.time_range*3600)
        self.source = hf.downloadService.addDownload(self.url)
        self.details_db_value_list = []
        
    def extractData(self):
        import matplotlib.colors as mcol
        import matplotlib.pyplot as plt
        my_cmap = plt.get_cmap('RdYlGn')
        my_cmap.set_over('w')
        data = {'direction' : self.link_direction, 'source_url' : self.source.getSourceUrl(), 'time_range' : self.time_range, 'request_timestamp' : self.time}
        
        x_line = self.time - 7200 #data with a timestamp greater than this one will be used for status evaluation
        #store the last qualities of the Tx links within those lists and evaluate them
        T0_link_list = []
        T1_link_list = []
        T2_link_list = []
        T3_link_list = []
        fobj = json.load(open(self.source.getTmpPath(), 'r'))['phedex']['link']
        
        for links in fobj:
            if links[self.link_direction] == self.your_name and links[self.parse_direction] not in self.blacklist:
                link_name = links[self.parse_direction]
                for transfer in links['transfer']:
                    help_append = {}
                    help_append['timebin'] = int(transfer['timebin'])
                    help_append['done_files'] = done = int(transfer['done_files'])
                    help_append['fail_files'] = fail = int(transfer['fail_files'])
                    help_append['rate'] = int(transfer['rate'])
                    help_append['name'] = link_name
                    if done != 0:
                        help_append['quality'] = float(done)/float(done + fail)
                        help_append['color'] = mcol.rgb2hex(my_cmap(help_append['quality']))
                        self.details_db_value_list.append(help_append)
                        
                        if help_append['timebin'] >= x_line:
                            if 'T0' in link_name:
                                T0_link_list.append(help_append['quality'])
                            elif 'T1' in link_name:
                                T1_link_list.append(help_append['quality'])
                            elif 'T2' in link_name:
                                T2_link_list.append(help_append['quality'])
                            elif 'T3' in link_name:
                                T3_link_list.append(help_append['quality'])
                                
                    elif fail != 0:
                        help_append['quality'] = 0.0
                        help_append['color'] = mcol.rgb2hex(my_cmap(help_append['quality']))
                        self.details_db_value_list.append(help_append)
                        
                        if help_append['timebin'] >= x_line:
                            if 'T0' in link_name:
                                T0_link_list.append(help_append['quality'])
                            elif 'T1' in link_name:
                                T1_link_list.append(help_append['quality'])
                            elif 'T2' in link_name:
                                T2_link_list.append(help_append['quality'])
                            elif 'T3' in link_name:
                                T3_link_list.append(help_append['quality'])
       
       
       # code for status evaluation TODO
        data['status'] = 1.0
        for i,eval_list in enumerate([T0_link_list, T1_link_list, T2_link_list, T3_link_list]):
            good_link = 0
            bad_link = 0
            for quality in eval_list:
                if quality < 0.3:   #here you could use a config parameter
                    bad_link += 1
                else:
                    good_link += 1
            if i == 0 and bad_link > 0: #here you could use a config parameter
                data['status'] = 0.0
                break
            elif i == 1 and bad_link > 0:
                if bad_link > 1:
                    data['status'] = 0.5
                elif bad_link > 2:
                    data['status'] = 0.0
                    break
            elif i == 2 and bad_link > 0:
                if float(good_link) / (bad_link + good_link) < 0.7:
                    data['status'] = 0.0
                    break
                elif float(good_link) / (bad_link + good_link) < 0.8:
                    data['status'] = 0.5
            elif i == 3 and bad_link > 0:
                if float(good_link) / (bad_link + good_link) < 0.7:
                    data['status'] = 0.5
                
        return data

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
            
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).order_by(self.subtables['details'].c.name.asc()).execute().fetchall()
        
        raw_data_list = [] #contains dicts {x,y,weight,fails,done,rate,time,color,link} where the weight determines the the color
        
        x0 = self.dataset['request_timestamp'] / 3600 * 3600 - self.dataset['time_range'] * 3600 #normalize the timestamps to the requested timerange
        y_value_map = {} # maps the name of a link to a y-value
        
        for values in details_list:
            if values['name'] not in y_value_map: #add a new entry if the link name is not in the value_map 
                y_value_map[values['name']] = len(y_value_map)
            help_dict = {'x':int(values['timebin']-x0)/3600, 'y':int(y_value_map[values['name']]), 'w':str('%.2f' %values['quality']), 'fails':int(values['fail_files']), 'done':int(values['done_files']), 'rate':str('%.3f' %(float(values['rate'])/1024/1024)), 'time':datetime.datetime.fromtimestamp(values['timebin']), 'color':values['color'], 'link':report_base + their_direction + values['name']}
            raw_data_list.append(help_dict)
        
        name_mapper = []
        
        for i in range(len(y_value_map)):
            name_mapper.append('-')
        
        for key in y_value_map.iterkeys():
            name_mapper[y_value_map[key]] = key
            
        for i,name in enumerate(name_mapper):
            name_mapper[i] = {'name':name, 'link':report_base + their_direction + name}
            
        data['link_list'] = raw_data_list
        data['titles'] = name_mapper
        data['height'] = len(y_value_map) * 15 + 100
        data['width'] = int(730/(self.dataset['time_range']+1))
        return data