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
        'linktest_url': ('use --no-check-certificate, end linktest url with starttime=', ''),
        'report_base':("insert base url for reports don't forget fromfilter or tofilter with your name! finish your link with starttime=, the starttime is inserted by the module ",'https://cmsweb.cern.ch/phedex/prod/Activity::ErrorInfo?tofilter=T1_DE_KIT&starttime='),
        'blacklist': ('ignore links from or to those sites, csv', ''),
        'your_name': ('Name of your site', 'T1_DE_KIT_Buffer'),
        'category': ('use prod or debug, its used to build links to the cern info sites','prod'),
        'button_pic_path_in': ('path to your in-button picture', '/HappyFace/gridka/static/themes/armin_box_arrows/trans_in.png'),
        'button_pic_path_out': ('path to your out-button picture', '/HappyFace/gridka/static/themes/armin_box_arrows/trans_out.png'),
        'qualitiy_broken_value': ('a timebin with a qualitiy equal or less than this will be considered as broken', '0.4'),
        'link_quality_eval_time': ('links within the last link_quality_eval_time hours will be used for link quality evaluation', '3'),
        
        'critical_average_quality': ('threshold for link confirmation', '0.1'),
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
        self.link_quality_eval_time = int(self.config['link_quality_eval_time'])
        self.quality_x_line = self.time-self.link_quality_eval_time*3600 #data with a timestamp greater than this one will be used for linkstatus confirmation
        self.url += str(self.time-self.time_range*3600)
        
        self.url1 = self.config['linktest_url']
        self.url1 += str(self.time-self.time_range*3600)
        self.source1 = hf.downloadService.addDownload(self.url1)
        self.critical_average_quality = float(self.config['critical_average_quality'])
        
        self.source = hf.downloadService.addDownload(self.url)
        self.source_url = self.source.getSourceUrl()+'\n'+self.source1.getSourceUrl()
        self.details_db_value_list = []

        self.category = self.config['category']
        self.button_pic_in = self.config['button_pic_path_in']
        self.button_pic_out = self.config['button_pic_path_out']
        self.qualitiy_broken_value = float(self.config['qualitiy_broken_value'])
    
    def confirmLinkStatus(self, link_name):
        crit_average = 0.1
        fobj1 = json.load(open(self.source1.getTmpPath(), 'r'))['phedex']['link']
        
        i = 0
        avg_quality = 0.0
        for links in fobj1:
            if links[self.parse_direction].startswith(link_name) and self.your_name not in links[self.link_direction]:
                for transfer in links['transfer']:
                    if transfer['quality'] != None and transfer['timebin'] >= self.quality_x_line:
                        i += 1
                        avg_quality += float(transfer['quality'])
        if i > 0:
            avg_quality = avg_quality/float(i)
            if avg_quality < self.critical_average_quality:
                link_status = 0
            else:
                link_status = 1
        else:
            link_status = 1
        
        return link_status
    
    def getPhedexConfigData(self):
        self.eval_time = int(self.config['eval_time'])
        self.time = int(time.time())/3600*3600
        
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
        
        self.unused_link_color = '#0000FF'
        self.color_map = ['#a50026', '#a90426', '#af0926', '#b30d26', '#b91326', '#bd1726', '#c21c27', '#c62027', '#cc2627', '#d22b27', '#d62f27', '#da362a', '#dc3b2c', '#e0422f', '#e24731', '#e54e35', '#e75337', '#eb5a3a', '#ee613e', '#f16640', '#f46d43', '#f57245', '#f67a49', '#f67f4b', '#f8864f', '#f98e52', '#f99355', '#fa9b58', '#fba05b', '#fca85e', '#fdad60', '#fdb365', '#fdb768', '#fdbd6d', '#fdc372', '#fdc776', '#fecc7b', '#fed07e', '#fed683', '#feda86', '#fee08b', '#fee28f', '#fee695', '#feea9b', '#feec9f', '#fff0a6', '#fff2aa', '#fff6b0', '#fff8b4', '#fffcba', '#feffbe', '#fbfdba', '#f7fcb4',\
        '#f4fab0', '#eff8aa', '#ecf7a6', '#e8f59f', '#e5f49b', '#e0f295', '#dcf08f', '#d9ef8b', '#d3ec87', '#cfeb85', '#c9e881', '#c5e67e', '#bfe47a', '#bbe278', '#b5df74', '#afdd70', '#abdb6d', '#a5d86a', '#a0d669', '#98d368', '#93d168', '#8ccd67', '#84ca66', '#7fc866', '#78c565', '#73c264', '#6bbf64', '#66bd63', '#5db961', '#57b65f', '#4eb15d', '#45ad5b', '#3faa59', '#36a657', '#30a356', '#279f53', '#219c52', '#199750', '#17934e', '#148e4b', '#118848', '#0f8446', '#0c7f43', '#0a7b41', '#07753e', '#05713c', '#026c39', '#006837']
    
    def getTierStatus(self, link_list):
        status = {}
        status['all'] = 1.0
        for tier,links in link_list.iteritems():
            status['%s' % tier] = 1.0
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
                if status['all'] != 0.0:
                    status['all'] = 0.0
            elif tier != 't0':
                metric = (2.0 * bad_link + warn_link) / (2.0 * bad_link + warn_link + good_link)
                sum_links = bad_link + warn_link + good_link
                if (metric >= self.critical_ratio[tier]) and (self.eval_amount[tier] <= (sum_links)):
                    if status['all'] != 0.0:
                        status['all'] = 0.0
                    status['%s' % tier] = 0.0
                elif (metric >= self.warning_ratio[tier]) and (self.eval_amount[tier] <= (sum_links)):
                    if status['all'] == 1.0:
                        status['all'] = 0.5
                    status['%s' % tier] = 0.5
        return status

    def extractData(self):
        
        self.getPhedexConfigData()
        
        data = {'direction' : self.link_direction, 'time_range' : self.time_range, 'request_timestamp' : self.time}

        #store the last N qualities of the Tx links within those dictionaries, {TX_xxx : (q1,q2,q3...)}

        link_list = {} # link_list['t1']['t1_de_kit'] == [{time1}, {time2}, ]
        fobj = json.load(open(self.source.getTmpPath(), 'r'))['phedex']['link']
        x_line = self.time - self.eval_time * 3600 #data with a timestamp greater than this one will be used for status evaluation

        for links in fobj:
            if links[self.link_direction].startswith(self.your_name) and links[self.parse_direction] not in self.blacklist:
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
                        help_append['color'] = self.color_map[int(help_append['quality']*100)]
                        
                        linkappend = 1
                        if (help_append['timebin'] >= self.quality_x_line) and (help_append['quality'] < self.critical_average_quality) and (not self.confirmLinkStatus(help_append['name'])):
                            help_append['color'] = self.unused_link_color
                            linkappend = 0
                        self.details_db_value_list.append(help_append)
                        if (help_append['timebin'] >= x_line) and linkappend:
                            link_list.setdefault(tier, {}).setdefault(link_name, []).append(help_append)
                    elif fail != 0:
                        help_append['quality'] = 0.0
                        help_append['color'] = self.color_map[int(help_append['quality']*100)]
                        
                        linkappend = 1
                        if help_append['timebin'] >= self.quality_x_line and not self.confirmLinkStatus(help_append['name']):
                            help_append['color'] = self.unused_link_color
                            linkappend = 0
                        self.details_db_value_list.append(help_append)
                        if help_append['timebin'] >= x_line and linkappend:
                            link_list.setdefault(tier, {}).setdefault(link_name, []).append(help_append)

        # code for status evaluation TODO: find a way to evaluate trend, change of quality between two bins etc.
        status = self.getTierStatus(link_list)
        data['status'] = status['all']
        
        return data

    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        
        self.getPhedexConfigData()
        
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
        x_list = {} #for Summary of the quality of all links at one time
        y_list = {} #for Summary of the quality of one link over different times
        
        x0 = self.dataset['request_timestamp'] / 3600 * 3600 - self.dataset['time_range'] * 3600 #normalize the timestamps to the requested timerange
        y_value_map = {} # maps the name of a link to a y-value
        x_line = self.dataset['request_timestamp'] - self.eval_time * 3600
        
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
            help_append = {'x': int(values['timebin']-x0)/3600, 'done_files': int(values['done_files']), 'fail_files': int(values['fail_files'])}
            raw_data_list.append(help_dict)
            if values['timebin'] >= x_line:
                y_list[help_dict['y']] = y_list.get(help_dict['y'], {})
                y_list[help_dict['y']][help_dict['x']] = help_dict['w']
            if (values['timebin'] >= x0):
                x_list.setdefault(help_dict['x'], {}).setdefault('t%s' % values['name'][1], {}).setdefault(values['name'], []).append(help_append)
        
        #create list for Summaries of the qualities of the links over different times
        y_summary = []
        for y_value in y_list:
            y_append_help = {'y': y_value}
            total = 0.0
            i = 0
            for x_value in y_list[y_value]:
                total += float(y_list[y_value][x_value])
                i += 1
            avg = total/float(i)
            y_append_help['color'] = self.color_map[int(avg*100)]
            y_append_help['quality'] = str('%.2f' % avg)
            y_summary.append(y_append_help)
        
        #create list for Summaries of the qualities of all links over one time
        
        x_summary = []
        for link_list in x_list:
            x_append_help = {'x': link_list}
            status = self.getTierStatus(x_list[link_list])
            for tier in status:
                x_append_help[tier] = status[tier]
                x_append_help['%s_color' % tier] = self.color_map[int(status[tier]*100)]
            x_summary.append(x_append_help)
        
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
        if self.config['link_direction'] == 'to':
            data['button_pic'] = self.config['button_pic_path_in']
            data['info_link'] = 'https://cmsweb.cern.ch/phedex/' + self.config['category'] + '/Activity::QualityPlots?graph=quality_all&entity=dest&src_filter='
        else:
            data['button_pic'] = self.config['button_pic_path_out']
            data['info_link'] = 'https://cmsweb.cern.ch/phedex/' + self.config['category'] + '/Activity::QualityPlots?graph=quality_all&entity=src&dest_filter='
        data['eval_time'] = 'last %s hrs' % self.eval_time
        data['y_summary'] = y_summary
        data['x_summary'] = x_summary
        return data