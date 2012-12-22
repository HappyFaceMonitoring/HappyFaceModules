import hf, logging
from sqlalchemy import *
from datetime import datetime,timedelta
from time import mktime
import modules.feedparser

class RSSFeed(hf.module.ModuleBase):
    config_keys = {
        'source': ('URL of the RSS feed', '')
#        'entries': ('Number of entries', '-1'),
#        'days': ('Show only entries from the last n days', '7'),
#        'hide_feed_title': ('Hide feed title', '0')
    }
    config_hint = ''
    
    table_columns = [
        Column('title', TEXT),
    ], []

    subtable_columns = {'feeds': ([
        Column('author', TEXT),
        Column('title', TEXT),
        Column('link', TEXT),
        Column('published', INT),
        Column('content', TEXT),
    ], [])}
    

    def prepareAcquisition(self):
        
        if 'source' not in self.config: raise hf.exceptions.ConfigError('source option not set!')
        self.source = hf.downloadService.addDownload(self.config['source'])
        
        self.status = 1.0

        self.details_db_value_list = []

    def extractData(self):
        
        data = {'source_url': self.source.getSourceUrl(),
                'status': self.status}
        
        feed = modules.feedparser.parse(self.source.getTmpPath())

        data['title'] = feed.feed.title

        # Sort entries by date
        try:
            feed.entries.sort(lambda x,y: cmp(y.published_parsed,x.published_parsed))
        except:
            pass

        entries = 0
        for entry in feed.entries:
            # TODO Skip entries older than ndays
            details_db_values = {}
            details_db_values['author'] = ''
            details_db_values['title'] = entry.title
            details_db_values['link'] = entry.link
            # Convert published time to unix time integer
            try:
                details_db_values['published'] = int(mktime(entry.published_parsed))
            except:
                details_db_values['published'] = 0
            details_db_values['content'] = entry.summary

            self.details_db_value_list.append(details_db_values)
            entries += 1
            # TODO only show n entries

        data['status'] = self.status

        return data
 
    def fillSubtables(self, parent_id):
        self.subtables['feeds'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        info_list = self.subtables['feeds'].select().where(self.subtables['feeds'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['feed_list'] = map(dict, info_list)

        return data
