import hf, logging
from sqlalchemy import *
from lxml.html import parse
from datetime import datetime

class dCacheDatasetRestoreLazy(hf.module.ModuleBase):
    config_keys = {
        'source': ('URL of the dCache Dataset Restore Monitor (Lazy)', '')
    }
    config_hint = ''

    def prepareAcquisition(self):
        
        if 'source' not in self.config: raise hf.exceptions.ConfigError('source option not set!')
        self.source = hf.downloadService.addDownload(self.config['source'])

        self.statusTagsOK = ['Pool2Pool','Staging']
        self.statusTagsFail = ['Waiting','Suspended','Unknown']

        self.total = 0
        self.total_problem = 0
        self.hit_retry = 0
        self.hit_time = 0
       
        self.details_db_value_list = []

    def extractData(self):
        
        data = {'source_url': self.source.getSourceUrl(),
                'total': 0,
                'total_problem': 0,
                'status': 1.0}

        if self.source.errorOccured() or not self.source.isDownloaded():
            data['error_string'] = 'Source file was not downloaded. Reason: %s' % self.source.error
            data['status'] = -1
            return data

        source_tree = parse(open(self.source.getTmpPath()))
        root = source_tree.getroot()

        stage_requests = []
        info = {}
        for td in root.findall('.//td'):
            tag = td.get('class')
            info[tag] = td.text
            if tag == 'path':
                stage_requests.append(info)
                info = {}

        self.total = len(stage_requests)
        data['total'] = self.total


        states = {}
        for tag in (self.statusTagsOK + self.statusTagsFail):
            states[tag] = 0

        
        for i in stage_requests:
            fail = False
            status = i['status'].split(' ')[0]
            if status in self.statusTagsFail:
                self.total_problem += 1
            states[status] += 1

            retries = int(i['retries'])
            # FIXME hardcoded retry limit
            if retries >= 2:
                self.hit_retry += 1
                fail = True
            # FIXME implement time limit
            started = datetime.strptime(i['started'],'%m.%d %H:%M:%S')

            details_db_values = {}
            details_db_values['pnfs'] = i['pnfs']
            details_db_values['path'] = i['path']
            details_db_values['retries'] = i['retries']
            details_db_values['status_short'] = status
            details_db_values['started_full'] = i['started']
            if fail:
                self.details_db_value_list.append(details_db_values)


        data['total_problem'] = self.total_problem
        data['hit_retry'] = self.hit_retry
        data['hit_time'] = self.hit_time
        
        for tag in (self.statusTagsOK + self.statusTagsFail):
            data['status_'+tag.lower()] = states[tag]

        return data

    def fillSubtables(self, parent_id):
        details_table.insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        info_list = details_table.select().where(details_table.c.parent_id==self.dataset['id']).execute().fetchall()
        data['info_list'] = map(dict, info_list)

        return data


module_table = hf.module.generateModuleTable(dCacheDatasetRestoreLazy, 'dcachedatasetrestorelazy', [
    Column('total', INT),
    Column('total_problem', INT),
    Column('status_pool2pool', INT),
    Column('status_staging', INT),
    Column('status_waiting', INT),
    Column('status_suspended', INT),
    Column('status_unknown', INT),
    Column('hit_retry', INT),
    Column('hit_time', INT),
])

details_table = hf.module.generateModuleSubtable('details', module_table, [
    Column('pnfs', TEXT),
    Column('path', TEXT),
    Column('started_full', TEXT),
    Column('retries', INT),
    Column('status_short', TEXT),
])

hf.module.addModuleClass(dCacheDatasetRestoreLazy)
