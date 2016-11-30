# -*- coding: utf-8 -*-
import hf
from sqlalchemy import TEXT, INT, Column
from lxml.html import parse
from string import strip

class dCacheMoverInfo(hf.module.ModuleBase):
    config_keys = {
        'watch_jobs': ('Colon separated list of the jobs to watch on the pools', ''),
        'pool_match_string': ('Watch only pools that match the given strings', 'rT_cms, rT_ops'),
        'critical_queue_threshold': ('Job is bad if the ratio of queued tasks to active tasks exceeds the threshold', '0.25'),
        'source': ('Download command for the qstat XML file', ''),
    }
    config_hint = ''

    table_columns = [
        Column('critical_queue_threshold', TEXT),
    ], []

    subtable_columns = {
        "summary": ([
        Column('job', TEXT),
        Column('active', INT),
        Column('max', INT),
        Column('queued', INT),
        ], []),

        "info": ([
        Column('pool', TEXT),
        Column('domain', TEXT),
        Column('job', TEXT),
        Column('active', INT),
        Column('max', INT),
        Column('queued', INT),
        ], []),
    }

    def prepareAcquisition(self):
        self.watch_jobs = map(strip, self.config['watch_jobs'].split(','))
        self.pool_match_string = map(strip, self.config['pool_match_string'].split(',')) 
        self.critical_queue_threshold = float(self.config['critical_queue_threshold'])

        if 'source' not in self.config: raise hf.exceptions.ConfigError('source option not set')
        self.source = hf.downloadService.addDownload(self.config['source'])
        self.source_url = self.source.getSourceUrl()

        self.job_info_db_value_list = []
        self.job_summary_db_value_list = []

    def extractData(self):
        data = {'critical_queue_threshold':self.critical_queue_threshold}

        source_tree = parse(open(self.source.getTmpPath()))
        root = source_tree.getroot()
        #take first tbody as table body with the information
        job_list = [] #list of jobs: gridftpq etc.

        for th in root.findall('.//th'):
            try:
                if th.get('colspan') == '3':
                    span = th.findall('span')[0]
                    job_list.append(span.text)
            except ValueError:
                pass
        root = root.findall('.//tbody')[0]

        #find all pools to be watched
        pool_list = []
        for tr in root.findall('.//tr'):
            spans = tr.findall('.//td')[0].findall('.//span')[0]
            bools = [group in spans.text for group in self.pool_match_string]
            if True in bools:
                pool_list.append(tr)

        #build summary list:
        help_dict = []
        for i in range(len(self.watch_jobs)):
            help_dict.append({'active': 0, 'max': 0, 'queued': 0})
        summary_dict = dict(zip(self.watch_jobs, help_dict))

        for pool in pool_list:
            tds = pool.findall('.//td')
            p_name = tds.pop(0).findall('.//span')[0].text
            p_domain = tds.pop(0).findall('.//span')[0].text
            job_tuples_list = [tds[x:x+3] for x in range(0, len(tds) - 3, 3)]
            for i, job in enumerate(job_tuples_list):
                if job_list[i] in self.watch_jobs:
                    append = {'pool': p_name,
                        'domain': p_domain,
                        'job':job_list[i],
                        'active': int(job[0].findall('.//span')[0].text),
                        'max': int(job[1].findall('.//span')[0].text),
                        'queued': int(job[2].findall('.//span')[0].text)
                    }
                    self.job_info_db_value_list.append(append)
                    summary_dict[job_list[i]]['active'] += int(job[0].findall('.//span')[0].text)
                    summary_dict[job_list[i]]['max'] += int(job[1].findall('.//span')[0].text)
                    summary_dict[job_list[i]]['queued'] += int(job[2].findall('.//span')[0].text)
        # calculate happiness as ratio of queued pools to total pools,
        # be sad if there is a critical queue
        data['status'] = 1.0
        for v in summary_dict.values():
            queue_ratio = v['queued'] / max(1, float(v['max']))
            if queue_ratio > 0:
                data['status'] = min(data['status'], 0.5)
            if queue_ratio > self.critical_queue_threshold:
                data['status'] = 0
        self.job_summary_db_value_list = [{'job':job, 'active':v['active'], 'max':v['max'], 'queued':v['queued']} for job,v in summary_dict.iteritems()]
        return data

    def fillSubtables(self, parent_id):
        self.subtables['info'].insert().execute([dict(parent_id=parent_id, **row) for row in self.job_info_db_value_list])
        self.subtables['summary'].insert().execute([dict(parent_id=parent_id, **row) for row in self.job_summary_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        info_list = self.subtables['info'].select().where(self.subtables['info'].c.parent_id==self.dataset['id']).execute().fetchall()
        info_list = map(dict, info_list)
        summary_list = self.subtables['summary'].select().where(self.subtables['summary'].c.parent_id==self.dataset['id']).execute().fetchall()
        summary_list = map(dict, summary_list)
        for group in summary_list:
            queue_ratio = group['queued'] / max(1, float(group['max']))
            if queue_ratio >= float(self.dataset['critical_queue_threshold']):
                group['status'] = 'critical'
            elif group['queued'] > 0:
                group['status'] = 'warning'
            else:
                group['status'] = 'ok'
        for group in info_list:
            queue_ratio = group['queued'] / max(1, float(group['max']))
            if queue_ratio >= float(self.dataset['critical_queue_threshold']):
                group['status'] = 'critical'
            elif group['queued'] > 0:
                group['status'] = 'warning'
            else:
                group['status'] = 'ok'

        data['summary_list'] = summary_list
        poollist = []
        for group in info_list:
            if not(group['pool'] in poollist):
                poollist.append(group['pool'])

        details_list = {}
        for group in poollist:
            appending = {group:[]}
            details_list.update(appending)

        for group in info_list:
            group['njobs'] = len(self.config['watch_jobs'].split(','))
            details_list[group['pool']].append(group)

        data['details_list'] = details_list

        return data
