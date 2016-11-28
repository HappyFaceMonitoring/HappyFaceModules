import hf
from sqlalchemy import TEXT, INT, Column
from datetime import timedelta
from time import mktime,time
import modules.feedparser

class RSSFeed(hf.module.ModuleBase):
    config_keys = {
        'source': ('URL of the RSS feed', ''),
        'entries': ('Number of entries ("-1" for all entries)', '-1'),
        'days': ('Show only entries from the last n days ("-1" for all days)', '7'),
        'hide_feed_title': ('Hide feed title', '0')
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
        self.source_url = self.source.getSourceUrl()
        try:
            self.entries = self.config['entries']
        except Exception:
            self.entries = -1
            raise hf.exceptions.ConfigError('entries option not set!')
        try:
            self.days = self.config['days']
        except Exception:
            self.days = -1
            raise hf.exceptions.ConfigError('days option not set!')
        try:
            self.hide_title = self.config['hide_feed_title']
        except Exception:
            self.hide_title = 0
            raise hf.exceptions.ConfigError('hide_feed_title option not set!')

        self.status = 1.0

        self.details_db_value_list = []

    def extractData(self):

        data = {'status': self.status}

        feed = modules.feedparser.parse(self.source.getTmpPath())

        data['title'] = feed.feed.title

        # Sort entries by date
        try:
            feed.entries.sort(lambda x,y: cmp(y.published_parsed,x.published_parsed))
        except Exception:
            pass

        if int(self.days) != -1:
            time_diff = timedelta(days=int(self.days)).total_seconds()
            best_before_time = time()-time_diff
        else:
            best_before_time = -1

        entries = 0
        detail_help_list = []
        for entry in feed.entries:
            details_db_values = {}
            details_db_values['author'] = entry.author
            #hide title if wanted
            if self.hide_title == 0:
                details_db_values['title'] = 'hide_title'
            else:
                details_db_values['title'] = entry.title
            details_db_values['link'] = entry.link
            # Convert published time to unix time integer
            try:
                details_db_values['published'] = int(mktime(entry.published_parsed))
            except Exception:
                details_db_values['published'] = 0
            details_db_values['content'] = entry.summary
            #Skip entries older than n days
            if details_db_values['published'] >= best_before_time:
                detail_help_list.append(details_db_values)
                entries += 1
        #only show n entries
        if int(self.entries) != -1:
            entries = int(self.entries)
            for i in range(0,int(self.entries)):
                self.details_db_value_list.append(detail_help_list[i])
        else:
            self.details_db_value_list = list(detail_help_list)

        data['status'] = self.status

        return data

    def fillSubtables(self, parent_id):
        self.subtables['feeds'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_db_value_list])

    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)

        info_list = self.subtables['feeds'].select().where(self.subtables['feeds'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['feed_list'] = map(dict, info_list)

        return data
