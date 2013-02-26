# -*- coding: utf-8 -*-
#
# Copyright 2012 Institut für Experimentelle Kernphysik - Karlsruher Institut für Technologie
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import hf, lxml, logging, datetime, lxml.html
from sqlalchemy import *
from lxml import etree
import lxml.html as ltml

class CMSSiteReadiness(hf.module.ModuleBase):
    
    config_keys = {
        'site_html': ('The CMS Web Site Readiness Report URL', ''),
        'tracked_site': ('Name of the site to display the readiness report', ''),
        'critical': ('Number of errors or warning that may occur until the site status is considered critical', '3'),
        'warning': ('Number of errors or warning that may occur until the site status might need attention', '2'),
    }
    config_hint = ''
    
    table_columns = [], []

    subtable_columns = {"rows" : ([Column("name", TEXT), Column("order", INT)] + \
        [Column("%02i_color"%i, TEXT) for i in xrange(1,11)] + [Column("%02i_link"%i, TEXT) for i in xrange(1,11)] + \
        [Column("%02i_data"%i, TEXT) for i in xrange(1,11)], []),
    }
    

    def prepareAcquisition(self):
        self.site_html = hf.downloadService.addDownload(self.config['site_html'])   #URL with source data
        self.tracked_site = str(self.config['tracked_site'])                        #name of the tracked table e.g. T1_DE_KIT
        #number of days to be shown on H.F.3 website: if zou want to change please replace all every number 11 shown in the script
        # number - 1 = shown days
        self.critical = int(self.config['critical'])                                
        self.warnings = int(self.config['warning'])
        self.data = {}
        self.giveback = {}
        
        self.giveback['source_url'] = self.site_html.getSourceUrl()
    
    def htmlcontent(self, tds):
        giveback = ''
        for aas in tds.iter('a'):
            if aas.text is not None:
                giveback += aas.text
        for divs in tds.iter('div'):
            if divs.text is not None:
                giveback += divs.text
        if giveback == '':
            return None
        return str(giveback).strip(' ')
        
    def extractData(self):
        tree = ltml.parse(open(self.site_html.getTmpPath()))
        ntable = ltml.fromstring('H')
        
        #get just the needed table from the html-file

        for tables in tree.iter('table'):
            for tds in tables.iter('td'):
                for divs in tds.iter('div'):
                    if divs.text == self.tracked_site:
                        ntable = tables
                        break
                        
        #extract needed data from KIT table and store in data with linked keyword
        #iterate about <tr> -> <td> -> <div> and extract data from div.text
        
        data_out = {}
        monthwarn = 0
        keycount = 0
        for trs in ntable.iter('tr'):
            key = ''
            keyset = 'unset'
            j = 0 
            for tds in trs.iter('td'):
                if self.htmlcontent(tds) is not None and keyset == 'unset':
                    if self.htmlcontent(tds) == self.tracked_site:
                        pass
                    elif self.htmlcontent(tds) == 'Site Readiness Status:':
                        key = self.htmlcontent(tds)
                        keyset = 'set'
                        data_out['key00'] = key
                        for j in range(6):
                            data_out[str(key) + '%02i' %j] = ' '
                        j += 1
                        keycount += 1
                    else:
                        try:
                            float(self.htmlcontent(tds))
                            key ='date'
                            data_out[str(key) + '%02i' %j] = ' '
                            data_out[str(key) + '_links%02i' %j] = 'none'
                            data_out[str(key) + '_col%02i' %j] = 'error'
                            keyset = 'set'
                            data_out['key' + '%02i' %keycount] = key
                            keycount += 1
                            j += 1
                            monthwarn = 1
                        except ValueError:
                            if monthwarn == 1:
                                key = 'month'
                                keyset = 'set'
                                data_out['key' + '%02i' %keycount] = key
                                data_out[str(key) + '%02i' %j] = self.htmlcontent(tds)
                                data_out[str(key) + '_links%02i' %j] = 'none'
                                data_out[str(key) + '_col%02i' %j] = 'error'
                                j += 1
                                keycount += 1
                            else:
                                key = str(self.htmlcontent(tds))
                                keyset = 'set'
                                data_out['key' + '%02i' %keycount] = key
                                keycount += 1
                elif keyset == 'set':
                    if self.htmlcontent(tds) is None:
                        data_out[str(key) + '%02i' %j] = ' '
                        data_out[str(key) + '_links%02i' %j] = 'none'
                        data_out[str(key) + '_col%02i' %j] = 'error'
                        j += 1
                    else:
                        data_out[str(key) + str(j)] = self.htmlcontent(tds)
                        
                        if 'href' in ltml.tostring(tds):
                            for links in tds.iterlinks():
                                data_out[str(key) + '_links%02i' %j] = links[2]
                        else:
                            data_out[str(key) + '_links%02i' %j] = 'none'
                        if 'green' in ltml.tostring(tds):
                            data_out[str(key) + '_col%02i' %j] = 'ok'
                        elif 'red' in ltml.tostring(tds):
                            data_out[str(key) + '_col%02i' %j] = 'critical'
                        elif 'yellow' in ltml.tostring(tds):
                            data_out[str(key) + '_col%02i' %j] = 'warning'
                        else:
                            data_out[str(key) + '_col%02i' %j] = 'error'
                        j += 1
                if key == 'date':
                    data_out['maxinput'] = j
        data_out['keycount'] = keycount
        
        self.data['name'] = []
        for count in range(int(data_out['keycount'])):
            self.data['name'].append(data_out['key%02i' %count])
        
        for count in range(int(int(data_out['maxinput']) - 11 + 1), int(data_out['maxinput'])):
            self.data['%02i_color'%int(count - int(data_out['maxinput']) + 11)] = []
            self.data['%02i_link'%int(count - int(data_out['maxinput']) + 11)] = []
            self.data['%02i_data'%int(count - int(data_out['maxinput']) + 11)] = []
            for count2 in self.data['name']:
                self.data['%02i_color'%int(count - int(data_out['maxinput']) + 11)].append(data_out[str(str(count2) + '_col%02i' %count)])
                self.data['%02i_link'%int(count - int(data_out['maxinput']) + 11)].append(data_out[str(str(count2) + '_links%02i' %count)])
                self.data['%02i_data'%int(count - int(data_out['maxinput']) + 11)].append(data_out[str(str(count2) + '%02i' %count)])
        
        #determine status of this module, need self.critical and self.warning if you don't show 10 days please change 09_color to (shown_days -1)_color 
        count = 0
        for i in self.data['09_color']:
            if i == 'yellow' or i == 'red':
                count += 1
        
        if count >= self.warnings:
            self.giveback['status'] = 0.5
        elif count >= self.critical:
            self.giveback['status'] = 0.0
        else:
            self.giveback['status'] = 1.0
            
        return self.giveback
        
    def fillSubtables(self, parent_id):
        def generate():
            l = len(self.data['01_color'])
            for i in xrange(l):
                yield dict(((key, val[i]) for key,val in self.data.iteritems()), order=i, parent_id=parent_id)
        self.subtables['rows'].insert().execute([k for k in generate()])
    
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        info_list = self.subtables['rows'].select().where(self.subtables['rows'].c.parent_id==self.dataset['id']).order_by(self.subtables['rows'].c.order.asc()).execute().fetchall()
        data['tabledata'] = map(dict, info_list)        
        return data
