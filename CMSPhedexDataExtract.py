# -*- coding: utf-8 -*-
import hf, lxml, logging, datetime
from sqlalchemy import *
import json
from string import strip
import time

class CMSPhedexDataExtract(hf.module.ModuleBase):
    
    config_keys = {
        'link_direction': ("transfers 'from' or 'to' you", 'to'),
        'time_range': ('set timerange in hours', '24'),
        'base_url': ('use --no-check-certificate, end base url with starttime=', ''),
        'report_base':("insert base url for reports don't forget fromfilter or tofilter with your name! finish your link with starttime=, the starttime is inserted by the module ",'https://cmsweb.cern.ch/phedex/prod/Activity::ErrorInfo?tofilter=T1_DE_KIT&starttime='),
        'blacklist': ('ignore links from or to those sites, csv', ''),
        'your_name': ('Name of your site', 'T1_DE_KIT_Buffer'),
        'category': ('use prod or debug, its used to build links to the cern info sites','prod'),
        'button_pic_path_in': ('path to your in-button picture', '/HappyFace/gridka/static/themes/armin_box_arrows/trans_in.png'),
        'button_pic_path_out': ('path to your out-button picture', '/HappyFace/gridka/static/themes/armin_box_arrows/trans_out.png'),
        'qualitiy_broken_value': ('a timebin with a qualitiy equal or less than this will be considered as broken', '0.4'),
        
        't0_critical_failures': ('failure threshold for status critical', '10'),
        't0_warning_failures': ('failure threshold for status warning', '10'),
        't1_critical_failures': ('failure threshold for status critical', '15'),
        't1_warning_failures': ('failure threshold for status warning', '10'),
        't2_critical_failures': ('failure threshold for status critical', '15'),
        't2_warning_failures': ('failure threshold for status warning', '10'),
        't3_critical_failures': ('failure threshold for status critical', '15'),
        't3_warning_failures': ('failure threshold for status warning', '10'),
        't0_critical_quality': ('quality threshold for status critical', '0.5'),
        't0_warning_quality': ('quality threshold for status warning', '0.6'),
        't1_critical_quality': ('quality threshold for status critical', '0.5'),
        't1_warning_quality': ('quality threshold for status warning', '0.6'),
        't2_critical_quality': ('quality threshold for status critical', '0.3'),
        't2_warning_quality': ('quality threshold for status warning', '0.5'),
        't3_critical_quality': ('quality threshold for status critical', '0.3'),
        't3_warning_quality': ('quality threshold for status warning', '0.5'),
        't1_critical_ratio': ('ratio of (2*broken_links+warning_links)/all_links threshold', '0.5'),
        't1_warning_ratio': ('ratio of (2*broken_links+warning_links)/all_links threshold', '0.3'),
        't2_critical_ratio': ('ratio of (2*broken_links+warning_links)/all_links threshold', '0.4'),
        't2_warning_ratio': ('ratio of (2*broken_links+warning_links)/all_links threshold', '0.3'),
        't3_warning_ratio': ('ratio of (2*broken_links+warning_links)/all_links threshold', '0.3'),
        't3_critical_ratio': ('ratio of (2*broken_links+warning_links)/all_links threshold', '0.5'),
        'eval_time': ('links within the last eval_time hours will be considered valuable status evaluation', '3'),
        't0_eval_amount': ('minimum amount of links to eval status for this link group', 0),
        't1_eval_amount': ('minimum amount of links to eval status for this link group', 0),
        't2_eval_amount': ('minimum amount of links to eval status for this link group', 5),
        't3_eval_amount': ('minimum amount of links to eval status for this link group', 5)
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
        self.eval_time = int(self.config['eval_time'])
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
        
        self.category = self.config['category']
        self.button_pic_in = self.config['button_pic_path_in']
        self.button_pic_out = self.config['button_pic_path_out']
        self.qualitiy_broken_value = float(self.config['qualitiy_broken_value'])

        self.critical_failures = {}
        self.warning_failures = {}
        self.critical_quality = {}
        self.warning_quality = {}
        self.critical_ratio = {}
        self.warning_ratio = {}
        self.eval_amount = {}
        for tier in ['t0', 't1', 't2', 't3']:
            self.critical_failures[tier] = int(self.config[tier + '_critical_failures'])
            self.warning_failures[tier] = int(self.config[tier + '_warning_failures'])
            self.critical_quality[tier] = float(self.config[tier + '_critical_quality'])
            self.warning_quality[tier] = float(self.config[tier + '_warning_quality'])
            self.eval_amount[tier] = int(self.config[tier + '_eval_amount'])
            if tier != 't0':
                self.critical_ratio[tier] =  float(self.config[tier + '_critical_ratio'])
                self.warning_ratio[tier] = float(self.config[tier + '_warning_ratio'])
                
    def extractData(self):
        #due to portability reasons this colormap is hardcoded produce a new colormap with: color_map = map(lambda i: matplotlib.colors.rgb2hex(matplotlib.pyplot.get_cmap('RdYlGn')(float(i)/100)), range(101)) 
        color_map = ['#a50026', '#a90426', '#af0926', '#b30d26', '#b91326', '#bd1726', '#c21c27', '#c62027', '#cc2627', '#d22b27', '#d62f27', '#da362a', '#dc3b2c', '#e0422f', '#e24731', '#e54e35', '#e75337', '#eb5a3a', '#ee613e', '#f16640', '#f46d43', '#f57245', '#f67a49', '#f67f4b', '#f8864f', '#f98e52', '#f99355', '#fa9b58', '#fba05b', '#fca85e', '#fdad60', '#fdb365', '#fdb768', '#fdbd6d', '#fdc372', '#fdc776', '#fecc7b', '#fed07e', '#fed683', '#feda86', '#fee08b', '#fee28f', '#fee695', '#feea9b', '#feec9f', '#fff0a6', '#fff2aa', '#fff6b0', '#fff8b4', '#fffcba', '#feffbe', '#fbfdba', '#f7fcb4',\
        '#f4fab0', '#eff8aa', '#ecf7a6', '#e8f59f', '#e5f49b', '#e0f295', '#dcf08f', '#d9ef8b', '#d3ec87', '#cfeb85', '#c9e881', '#c5e67e', '#bfe47a', '#bbe278', '#b5df74', '#afdd70', '#abdb6d', '#a5d86a', '#a0d669', '#98d368', '#93d168', '#8ccd67', '#84ca66', '#7fc866', '#78c565', '#73c264', '#6bbf64', '#66bd63', '#5db961', '#57b65f', '#4eb15d', '#45ad5b', '#3faa59', '#36a657', '#30a356', '#279f53', '#219c52', '#199750', '#17934e', '#148e4b', '#118848', '#0f8446', '#0c7f43', '#0a7b41', '#07753e', '#05713c', '#026c39', '#006837']
        data = {'direction' : self.link_direction, 'source_url' : self.source.getSourceUrl(), 'time_range' : self.time_range, 'request_timestamp' : self.time}
        
        x_line = self.time - self.eval_time * 3600 #data with a timestamp greater than this one will be used for status evaluation
        #store the last N qualities of the Tx links within those dictionaries, {TX_xxx : (q1,q2,q3...)}

        link_list = {} # link_list['t1']['t1_de_kit'] == [{time1}, {time2}, ]
        fobj = json.load(open(self.source.getTmpPath(), 'r'))['phedex']['link']
        
        for links in fobj:
            if links[self.link_direction] == self.your_name and links[self.parse_direction] not in self.blacklist:
                link_name = links[self.parse_direction]
                tier = 't' + link_name[1]
                for transfer in links['transfer']:
                    help_append = {}
                    help_append['timebin'] = int(transfer['timebin'])
                    help_append['done_files'] = done = int(transfer['done_files'])
                    help_append['fail_files'] = fail = int(transfer['fail_files'])
                    help_append['rate'] = int(transfer['rate'])
                    help_append['name'] = link_name
                    #quality = done_files/(done_files + fail_files), if else to catch ZeroDivisionError
                    if done != 0:
                        help_append['quality'] = float(done)/float(done + fail)
                        help_append['color'] = color_map[int(help_append['quality']*100)]
                        self.details_db_value_list.append(help_append)
                        if help_append['timebin'] >= x_line:
                            link_list.setdefault(tier, {}).setdefault(link_name, []).append(help_append)
                    elif fail != 0:
                        help_append['quality'] = 0.0
                        help_append['color'] = color_map[int(help_append['quality']*100)]
                        self.details_db_value_list.append(help_append)
                        if help_append['timebin'] >= x_line:
                            link_list.setdefault(tier, {}).setdefault(link_name, []).append(help_append)
       
       # code for status evaluation TODO: find a way to evaluate trend, change of quality between two bins etc.
        data['status'] = 1.0
        for tier,links in link_list.iteritems():
            good_link = 0
            bad_link = 0
            warn_link = 0
            for  link_name, time_bins in links.iteritems():
                try:
                    done_files = 0
                    fail_files = 0
                    for single_bin in time_bins:
                        done_files += int(single_bin['done_files'])
                        fail_files += int(single_bin['fail_files'])
                    if fail_files != 0 and (float(done_files) / (done_files + fail_files) <= self.critical_quality[tier] or fail_files >= self.critical_failures[tier]):
                        bad_link += 1
                    elif fail_files != 0 and (float(done_files) / (done_files + fail_files) <= self.warning_quality[tier] or fail_files >= self.warning_failures[tier]):
                        warn_link += 1
                    elif done_files != 0:
                        good_link += 1
                except IndexError:
                    pass
            if tier == 't0' and bad_link > 0: #here you could use a config parameter
                data['status'] = 0.0
                break
            elif tier != 't0':
                if ((2.0 * bad_link + warn_link) / (2.0 * bad_link + warn_link + good_link) >= self.critical_ratio[tier]) and self.eval_amount[tier] <= (bad_link + warn_link + good_link):
                    data['status'] = 0.0
                    break
                elif ((2.0 * bad_link + warn_link) / (2.0 * bad_link + warn_link + good_link) >= self.warning_ratio[tier]) and self.eval_amount[tier] <= (bad_link + warn_link + good_link):
                    data['status'] = 0.5
        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])
    
    def getTemplateData(self):
        
        report_base = strip(self.config['report_base']) + '&'
        your_direction = strip(self.config['link_direction'])
        
        if your_direction == 'from':
            their_direction = 'tofilter='
            your_direction = 'fromfilter='
        else:
            their_direction = 'fromfilter='
            your_direction = 'tofilter='
            
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).order_by(self.subtables['details'].c.name.asc()).execute().fetchall()
            
        raw_data_list = [] #contains dicts {x,y,weight,fails,done,rate,time,color,link} where the weight determines the the color
        
        x0 = self.dataset['request_timestamp'] / 3600 * 3600 - self.dataset['time_range'] * 3600 #normalize the timestamps to the requested timerange
        y_value_map = {} # maps the name of a link to a y-value
        for values in details_list:
            if values['name'] not in y_value_map: #add a new entry if the link name is not in the value_map 
                y_value_map[values['name']] = len(y_value_map)
            t_number = values['name'].split('_')[0].lower()
            marking_color = values['color']
            if int(self.config['%s_critical_failures'%t_number]) <= int(values['fail_files']):
                marking_color = '#ff0000'
            elif int(self.config['%s_warning_failures'%t_number]) <= int(values['fail_files']):
                marking_color = '#af00af'
            help_dict = {'x':int(values['timebin']-x0)/3600, 'y':int(y_value_map[values['name']]), 'w':str('%.2f' %values['quality']), 'fails':int(values['fail_files']), 'done':int(values['done_files']), 'rate':str('%.3f' %(float(values['rate'])/1024/1024)), 'time':datetime.datetime.fromtimestamp(values['timebin']), 'color':values['color'], 'link':report_base + their_direction + values['name'], 'marking':marking_color}
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
        data['width'] = int(660/(self.dataset['time_range']+1))
        data['button_pic_in'] = self.config['button_pic_path_in']
        data['button_pic_out'] = self.config['button_pic_path_out']
        data['info_link_1'] = 'https://cmsweb.cern.ch/phedex/' + self.config['category'] + '/Activity::QualityPlots?graph=quality_all&entity=dest&src_filter='
        data['info_link_2'] = 'https://cmsweb.cern.ch/phedex/' + self.config['category'] + '/Activity::QualityPlots?graph=quality_all&entity=src&dest_filter='
        return data