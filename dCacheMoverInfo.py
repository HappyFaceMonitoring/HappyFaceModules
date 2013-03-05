# -*- coding: utf-8 -*-
import time, re, HTMLParser
import hf, lxml, logging, datetime
from sqlalchemy import *
from lxml import etree

class dCacheMoverInfo(hf.module.ModuleBase):
    config_keys = {
        'watch_jobs': ('Colon separated list of the jobs to watch on the pools', ''),
        'pool_match_regex': ('Watch only pools that match the given regular expression', 'rT_cms$'),
        'critical_queue_threshold': ('Job is bad if the number of queued tasks exceeds the threshold', '6'),
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
        self.watch_jobs = self.config['watch_jobs'].split(',')
        self.pool_match_regex = self.config['pool_match_regex'] 
        self.critical_queue_threshold = self.config['critical_queue_threshold']
        
        if 'source' not in self.config: raise hf.exceptions.ConfigError('source option not set')
        self.source = hf.downloadService.addDownload(self.config['source'])
        
        self.job_info_db_value_list = []
        self.job_summary_db_value_list = []
    
    def extractData(self):
        data = {'critical_queue_threshold':self.critical_queue_threshold}
        data['source_url'] = self.source.getSourceUrl()


        class TableRowExtractor(HTMLParser.HTMLParser):
            '''
            Parse the HTML and extract all rows from the table.
            The format is a list of rows, each row is a list with format [th?, class, data],
            saved in the extractedRows attribute
            '''
            extractedRows = []
            __currentRow = []
            __curTag = ''
            def handle_starttag(self, tag, attr):
                self.__curTag = tag
                if tag == "tr":
                    self.__currentRow = []
                elif tag == 'td' or tag == 'th':
                    cssClass = ''
                    for a in attr:
                        if a[0] == 'class': cssClass = a[1]
                    self.__currentRow.append([tag == 'th', cssClass, ''])
                    
            def handle_endtag(self, tag):
                    if tag == "tr":
                        self.extractedRows.append(self.__currentRow)
                        self.__currentRow = []
                        
            def handle_data(self, data):
                if data == '\n' or data == '\r\n' or data == '':
                    return
                if self.__curTag == 'td' or self.__curTag == 'th':
                    self.__currentRow[len(self.__currentRow)-1][2] = data

        def extractPools(rows):
            '''
            This applies 'filters' to the row-data to discard headlines, totals
            and extract only pools that are interessting for us.
            
            Return value is a dictionary of pools, key is pool name, value
            is a tuple (domain, value-dict). Value-dict contains the data, key
            is the transfer-type, value is a tripple (cur, max, queue)
            '''
            protocols = []
            # extract all protocols (they are in the first row, starting at third column
            for p in rows[0][2:]:
                protocols.append(p[2])
            
            pools = {}
            for r in rows:
                # Discard empty rows
                if len(r) == 0: continue
                # Discard all rows starting with a head
                if r[0][0]: continue
                
                # We now    have data-rows only.
                name, domain = r[0][2], r[1][2]
                
                # Only CMS read-tape pools
                if not re.search(self.pool_match_regex, name):
                    #print 'Discard', name
                    continue
                values = {}
                for i,proto in enumerate(protocols):
                    values[proto] = (int(r[i*3+2][2]), int(r[i*3+3][2]), int(r[i*3+4][2]))
                    
                pools[name] = (domain, values)
            return pools
        # now actually import the data
        tableRowExtractor = TableRowExtractor()
        for line in open(self.source.getTmpPath(), 'r'):
            tableRowExtractor.feed(line)
        pool_list = extractPools(tableRowExtractor.extractedRows)
        
        num_queuing_pools = 0
        has_critical_queue = False
        
        job_transfers_sum = {} # calculate sums over all pools
        
        for pool,value in pool_list.iteritems():
            job_has_queue = False
            # Add all the job-values that interesst us to database as a new row per job
            for job in self.watch_jobs:
                if not job in job_transfers_sum: job_transfers_sum[job] = [0, 0, 0]
                job_info_db_values = {}
                job_info_db_values['pool'] = pool
                job_info_db_values['domain'] = value[0]
                job_info_db_values['job'] = job
                job_info_db_values['active'] = int(value[1][job][0])
                job_info_db_values['max'] = int(value[1][job][1])
                job_info_db_values['queued'] = int(value[1][job][2])
                self.job_info_db_value_list.append(job_info_db_values)
                
                job_transfers_sum[job][0] += job_info_db_values['active']
                job_transfers_sum[job][1] += job_info_db_values['max']
                job_transfers_sum[job][2] += job_info_db_values['queued']

                if int(value[1][job][2]) > 0:
                    job_has_queue = True
                elif int(value[1][job][2]) > self.critical_queue_threshold:
                    has_critical_queue = True
            if job_has_queue:
                num_queuing_pools += 1
        # calculate happiness as ratio of queued pools to total pools,
        # be sad if there is a critical queue
        data['status'] = 1.0 - float(num_queuing_pools) / len(pool_list)
        if has_critical_queue: data['status'] = 0.0
        self.job_summary_db_value_list = [{'job':job, 'active':v[0], 'max':v[1], 'queued':v[2]} for job,v in job_transfers_sum.iteritems()]
        
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
        for i,group in enumerate(summary_list):
            if group['queued'] >= int(self.dataset['critical_queue_threshold']):
                group['status'] = 'critical'
            elif group['queued'] > 0:
                group['status'] = 'warning'
            else:
                group['status'] = 'ok'
        for i,group in enumerate(info_list):
            if group['queued'] >= int(self.dataset['critical_queue_threshold']):
                group['status'] = 'critical'
            elif group['queued'] > 0:
                group['status'] = 'warning'
            else:
                group['status'] = 'ok'
            
        data['summary_list'] = summary_list
        poollist = []
        for i,group in enumerate(info_list):
            if not(group['pool'] in poollist):
                poollist.append(group['pool'])
                
        details_list = {}
        for i,group in enumerate(poollist):
            appending = {group:[]}
            details_list.update(appending)
        
        for i,group in enumerate(info_list):
            details_list[group['pool']].append(group)
        
        data['details_list'] = details_list
        
        return data
        