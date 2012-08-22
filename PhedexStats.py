import hf, logging
from sqlalchemy import *
from lxml import etree

class PhedexStats(hf.module.ModuleBase):
    config_keys = {
        'phedex_xml': ('URL of the PhEDEx XML file', '')
    }
    config_hint = ''

    def prepareAcquisition(self):

        if 'phedex_xml' not in self.config: raise hf.exceptions.ConfigError('phedex_xml option not set')
        self.phedex_xml = hf.downloadService.addDownload(self.config['phedex_xml'])

        self.details_db_value_list = []

    def extractData(self):
        data = {'source_url': self.phedex_xml.getSourceUrl(),
                'startlocaltime': '',
                'endlocaltime': '',
                'failed_transfers': '',
                'status': 1.0}

        if self.phedex_xml.errorOccured() or not self.phedex_xml.isDownloaded():
            data['error_string'] = 'Source file was not downloaded. Reason: %s' % self.phedex_xml.error
            data['status'] = -1
            return data

        source_tree = etree.parse(open(self.phedex_xml.getTmpPath()))
        root = source_tree.getroot()

        self.startlocaltime = root.get('startlocaltime')
        self.endlocaltime = root.get('endlocaltime')

        data['startlocaltime'] = self.startlocaltime
        data['endlocaltime'] = self.endlocaltime

        failed_transfers = 0

        for fromsite in root.findall('fromsite'):
            for tosite in fromsite:
                for reason in tosite:
                    details_db_values = {}
                    details_db_values['site_name'] = tosite.get('name')
                    details_db_values['number'] = int(reason.get('n'))
                    details_db_values['origin'] = reason.get('origin')
                    details_db_values['error_message'] = reason.text

                    failed_transfers += int(reason.get('n'))
                    self.details_db_value_list.append(details_db_values)

        data['failed_transfers'] = failed_transfers

        return data

    def fillSubtables(self, parent_id):
        details_table.insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        info_list = details_table.select().where(details_table.c.parent_id==self.dataset['id']).execute().fetchall()
        data['info_list'] = map(dict, info_list)

        return data


module_table = hf.module.generateModuleTable(PhedexStats, 'phedex_stats', [
    Column('startlocaltime', TEXT),
    Column('endlocaltime', TEXT),
    Column('failed_transfers', INT),
])

details_table = hf.module.generateModuleSubtable('details', module_table, [
    Column('site_name', TEXT),
    Column('number', INT),
    Column('origin', TEXT),
    Column('error_message', TEXT),
])

hf.module.addModuleClass(PhedexStats)
