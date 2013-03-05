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

import hf, lxml, logging, datetime
from sqlalchemy import *
from lxml import etree
from string import strip

class Summary(hf.module.ModuleBase):
    config_keys = {
        'site_keys': ('Colon separated list of site names. The values are prefixes for the actual url and category configuration. For example use desy', 'desy'),
        'categories': ('Colon separated list of HF categories to display', ''),
        'SITE_KEY': ('URL of a HappyFace XML output. Replace SITE_KEY with a key from site_keys like desy_site', ''),
        'SITE_CAT': ('title of categories to be parsed, replace SITE_CAT with a key from site_keys like desy_cat',''),
        'pic_path': ('path to local folder with the images for hf3.','/HappyFace/gridka/static/themes/armin_box_arrows/')
    }
    config_hint = 'The big problem is that different sites use different titles for their categories, so its necessary to define a list of categories for each site, make sure to use the same order for all sites!'
    
    table_columns = [], []   

    subtable_columns = {'details': ([
            Column("site", TEXT),
            Column("catname", TEXT),
            Column("cattitle", TEXT),
            Column("type", TEXT),
            Column("status", FLOAT),
            Column("catlink", TEXT),
            Column("order", INT)
    ], []),}
    
    def prepareAcquisition(self):

        # definition of the database table keys and pre-defined values
            self.site_keys = map(strip, self.config['site_keys'].lower().split(','))
            self.order = map(strip, self.config['site_keys'].split(','))
            self.sites = []
            self.cats = []
            self.details_db_value_list = []
            for i,site in enumerate(self.site_keys):
                self.sites.append(hf.downloadService.addDownload(self.config[str(self.site_keys[i]) + '_site']))
                self.cats.append(map(strip, self.config[str(self.site_keys[i]) + '_cat'].split(',')))
        
    def extractData(self):
        data = {}
        data['status'] = 1
        for i,source_key in enumerate(self.sites):
            source = etree.parse(open(source_key.getTmpPath()))
            root = source.getroot()
            for cat in root:
                if cat.tag == 'category':
                    details_db_value = {}
                    details_db_value['site'] = self.site_keys[i]
                    for cat_item in cat:
                        if cat_item.tag == 'name':
                            details_db_value['catname'] = cat_item.text
                        if cat_item.tag == 'title':
                            details_db_value['cattitle'] = cat_item.text
                        if cat_item.tag == 'type':
                            details_db_value['cattype'] = cat_item.text
                        if cat_item.tag == 'link':
                            details_db_value['catlink'] = cat_item.text
                        if cat_item.tag == 'status':
                            details_db_value['status'] = float(cat_item.text)
                        
                    for j,allowed in enumerate(self.cats[i]):
                        if allowed == details_db_value['cattitle']:
                            details_db_value['order'] = 1000*i + j
                            self.details_db_value_list.append(details_db_value)
        
        return data
            
    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])
    
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).order_by(self.subtables['details'].c.order.asc()).execute().fetchall()
        details_list = map(dict, details_list)
        pic_path = self.config['pic_path']
        sorted_list = [[]]
        last = 0
        for i,cats in enumerate(details_list):
            act = int(cats['order']/1000)
            if cats['status'] >= 0.66:
                cats['status'] = pic_path + "mod_happy.png" 
            elif cats['status'] >= 0.33:
                cats['status'] = pic_path + "mod_neutral.png" 
            else:
                cats['status'] = pic_path + "mod_unhappy.png"
            if act != last:
                last += 1
                sorted_list.append([])
                sorted_list[last].append(cats)
            else:
                sorted_list[last].append(cats)
        data['details'] = sorted_list
        data['main_names'] = map(strip, self.config['site_keys'].split(','))
        data['cat_header'] = map(strip, self.config['categories'].split(','))
        return data
                        