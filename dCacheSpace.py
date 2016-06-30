import hf
from sqlalchemy import Column, TEXT, INT, FLOAT
import lxml.html
import StringIO

def rare_to_TB(rare_value):
    tb_value = rare_value/(10.0)**12
    return round(tb_value,1)

class dCacheSpace(hf.module.ModuleBase):
    config_keys = {
        'xml_source': ('URL of the XML file', '')
    }
    config_hint = ''
    table_columns = [
        Column('timestamp', INT)   #stays in!!
    ], []

    subtable_columns = {'details': ([
        Column('group', TEXT),
        Column('pool', TEXT),
        Column('noloc_files', INT),
        Column('noloc_size', FLOAT),
        Column('nosize_files', INT),
        Column('replica_files', INT),
        Column('replica_size', FLOAT),
        Column('total_files', INT),
        Column('total_size', FLOAT),
        Column('unique_files', INT),
        Column('unique_size', FLOAT),
        Column('unknown_files', INT)
    ], []),}

    def prepareAcquisition(self):

        if 'xml_source' not in self.config: raise hf.exceptions.ConfigError('xml_source option not set')
        self.xml_source = hf.downloadService.addDownload(self.config['xml_source'])
        self.source_url = self.xml_source.getSourceUrl()
        self.details_db_value_list = []

    def extractData(self):
        data={'timestamp': 0}

        webpage = open(self.xml_source.getTmpPath())
        strwebpage = webpage.read()
        tree = lxml.html.parse(StringIO.StringIO(strwebpage))

        cur_timestamp = 0
        try:
            cur_timestamp = int(map(lambda date: date.text, tree.iter('date'))[0])
        except Exception:
            cur_timestamp = -1
        data['timestamp'] = cur_timestamp
        
        for group in tree.iter('group'):
            for pool in group:
                append_help = {}
                for files in pool:
                    append_help[group.tag] = group.attrib['name']
                    append_help[pool.tag] = pool.attrib['name']
                    if 'size' in files.tag:
                        append_help[files.tag] = rare_to_TB(int(files.text))
                    else:
                        append_help[files.tag] = int(files.text)
                if append_help != {}:
                    self.details_db_value_list.append(append_help)
        return data
    
    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])
        
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).order_by(self.subtables['details'].c.group.asc()).execute().fetchall()
        
        table_dict = {}
        for line in map(dict, details_list):
            for key in line:
                if 'parent' not in key and '_' in key:
                    val = line[key]
                    if val != None:
                        if line['pool'] == 'allpools':
                            table_dict.setdefault(line['group'], {}). \
                                setdefault(line['pool'], {}). \
                                setdefault('%s (all)' % key.split('_')[0], {})[key.split('_')[1]] = val
                        else:
                            table_dict.setdefault(line['group'], {}). \
                                setdefault(line['pool'], {}). \
                                setdefault('%s (%s)' % (key.split('_')[0], line['pool'].split('_')[1]), {})[key.split('_')[1]] = val
        data['table_dict'] = table_dict
        
        return data
