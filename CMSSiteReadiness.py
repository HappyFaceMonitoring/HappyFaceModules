# -*- coding: utf-8 -*-
import hf, lxml, logging, datetime, lxml.html
from sqlalchemy import *
from lxml import etree
import lxml.html as ltml

class CMSSiteReadiness(hf.module.ModuleBase):
    
    def prepareAcquisition(self):
        self.site_html = hf.downloadService.addDownload(self.config['site_html'])
        self.tracked_site = str(self.config['tracked_site'])
        self.data = {}
        self.autschn = []
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
                            data_out[str(key) + '%02i' %j] = 'none'
                        j += 1
                        keycount += 1
                    else:
                        try:
                            float(self.htmlcontent(tds))
                            key ='date'
                            data_out[str(key) + '%02i' %j] = 'none'
                            data_out[str(key) + '_links%02i' %j] = 'none'
                            data_out[str(key) + '_col%02i' %j] = 'none'
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
                                data_out[str(key) + '_col%02i' %j] = 'none'
                                j += 1
                                keycount += 1
                            else:
                                key = str(self.htmlcontent(tds))
                                keyset = 'set'
                                data_out['key' + '%02i' %keycount] = key
                                keycount += 1
                elif keyset == 'set':
                    if self.htmlcontent(tds) is None:
                        data_out[str(key) + '%02i' %j] = 'none'
                        data_out[str(key) + '_links%02i' %j] = 'none'
                        data_out[str(key) + '_col%02i' %j] = 'none'
                        j += 1
                    else:
                        data_out[str(key) + str(j)] = self.htmlcontent(tds)
                        
                        if 'href' in ltml.tostring(tds):
                            for links in tds.iterlinks():
                                data_out[str(key) + '_links%02i' %j] = links[2]
                        else:
                            data_out[str(key) + '_links%02i' %j] = 'none'
                        if 'green' in ltml.tostring(tds):
                            data_out[str(key) + '_col%02i' %j] = 'green'
                        elif 'red' in ltml.tostring(tds):
                            data_out[str(key) + '_col%02i' %j] = 'red'
                        elif 'yellow' in ltml.tostring(tds):
                            data_out[str(key) + '_col%02i' %j] = 'yellow'
                        else:
                            data_out[str(key) + '_col%02i' %j] = 'none'
                        j += 1
                if key == 'date':
                    data_out['maxinput'] = j
        data_out['keycount'] = keycount
        
        self.data['name'] = []
        for count in range(int(data_out['keycount'])):
            self.data['name'].append(data_out['key%02i' %count])
        
        for count in range(int(int(data_out['maxinput']) - 10), int(data_out['maxinput'])):
            self.data['%02i_color'%int(count - int(data_out['maxinput']) + 11)] = []
            self.data['%02i_link'%int(count - int(data_out['maxinput']) + 11)] = []
            self.data['%02i_data'%int(count - int(data_out['maxinput']) + 11)] = []
            for count2 in self.data['name']:
                self.data['%02i_color'%int(count - int(data_out['maxinput']) + 11)].append(data_out[str(str(count2) + '_col%02i' %count)])
                self.data['%02i_link'%int(count - int(data_out['maxinput']) + 11)].append(data_out[str(str(count2) + '_links%02i' %count)])
                self.data['%02i_data'%int(count - int(data_out['maxinput']) + 11)].append(data_out[str(str(count2) + '%02i' %count)])
        backpack = {}
        backpack['status'] = 1
        
        return backpack
        
    def fillSubtables(self, parent_id):
        def generate():
            l = len(self.data['01_color'])
            for i in xrange(l):
                yield dict(((key, val[i]) for key,val in self.data.iteritems()), order=i, parent_id=parent_id)
        details_table.insert().execute([k for k in generate()])
    
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        info_list = details_table.select().where(details_table.c.parent_id==self.dataset['id']).execute().fetchall()
        data['tabledata'] = map(dict, info_list)
        return data

hf.module.addModuleClass(CMSSiteReadiness)
module_table = hf.module.generateModuleTable(CMSSiteReadiness, "cms_site_readiness", [
])

details_table = hf.module.generateModuleSubtable("rows", module_table, [Column("name", TEXT), Column("order", INT)] + \
    [Column("%02i_color"%i, TEXT) for i in xrange(1,11)] + [Column("%02i_link"%i, TEXT) for i in xrange(1,11)] + \
    [Column("%02i_data"%i, TEXT) for i in xrange(1,11)]
    )