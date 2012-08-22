# -*- coding: utf-8 -*-
import hf, lxml, logging, datetime
from sqlalchemy import *
from lxml import etree


class Summary(hf.module.ModuleBase):
    config_keys = {
        'site_keys': ('Colon separated list of site names.\nThe name corresponds to a config entry with the actual URL', ''),
        'categories': ('Colon separated list of HF categories to display', ''),
        'SITE_KEY': ('URL of a HappyFace XML output.\nReplace SITE_KEY by whatever you specified in site_keys', ''),
    }
    config_hint = ''
    def prepareAcquisition(self):

        # definition of the database table keys and pre-defined values
        try:
            self.site_keys = self.config['site_keys'].split(',')
            self.sites = []
            self.source = ''
            self.details_db_value_list = []
            for i,site in enumerate(self.site_keys):
                self.sites.append(hf.downloadService.addDownload(self.config[self.site_keys[i]]))
                self.source += str(self.site_keys[i]) + ': ' + str(self.sites[i].getSourceUrl()) + '\n'
            self.categories = self.config['categories'].split(',')
        except KeyError, ex:
            raise hf.exceptions.ConfigError('Required parameter "%s" not specified' % str(e))
        
    def extractData(self):
        data = {}
        data['source_url'] = self.source
        data['status'] = 1
        count = 0
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
                    if details_db_value['cattitle'] in self.categories:
                        details_db_value['order'] = int(count)
                        self.details_db_value_list.append(details_db_value)
                        count = count + 1
        return data
            
    def fillSubtables(self, parent_id):
        details_table.insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])
    
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = details_table.select().where(details_table.c.parent_id==self.dataset['id']).order_by(details_table.c.order.asc()).execute().fetchall()
        rawhelpdata = map(dict, details_list)
        helpdata = {}
        tempsite = ''
        key_order = []
        count = -1
        
        for i,cat in enumerate(rawhelpdata):
            if cat['site'] == tempsite:
                helpdata['%02i' %count].append(cat)
            else:
                count += 1
                helpdata['%02i' %count] = []
                tempsite = cat['site']
                helpdata['%02i' %count].append(cat)
        for i,keys in enumerate(helpdata['00']):
            key_order.append(keys['cattitle'])
            
        for c,rows in helpdata.iteritems():
            appender = []
            for i,keys in enumerate(key_order):
                for z,cols in enumerate(rows):
                    if cols['cattitle'] == keys:
                        if cols['status'] >= 0.66:
                           cols['status'] = "/static/themes/armin_box_arrows/mod_happy.png" 
                        elif cols['status'] >= 0.33:
                           cols['status'] = "/static/themes/armin_box_arrows/mod_neutral.png" 
                        else:
                           cols['status'] = "/static/themes/armin_box_arrows/mod_unhappy.png" 
                        appender.append(cols)
            data['_' + c] = appender
        return data
                        

module_table = hf.module.generateModuleTable(Summary, "summary", [
])        

details_table = hf.module.generateModuleSubtable('details', module_table, [
        Column("site", TEXT),
        Column("catname", TEXT),
        Column("cattitle", TEXT),
        Column("type", TEXT),
        Column("status", FLOAT),
        Column("catlink", TEXT),
        Column("order", INT)
])
hf.module.addModuleClass(Summary)