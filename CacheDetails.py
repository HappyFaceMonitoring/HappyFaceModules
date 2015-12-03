# Module for Status of CMS6 via condor_q

import hf
from sqlalchemy import *
import json
import itertools
import time


class CacheDetails(hf.module.ModuleBase):
    config_keys = {'sourceurl': ('Source Url', ''),
                   'plotsize_x': ('size of the plot in x', '10'),
                   'plotsize_y': ('size of plot in y', '5'),
                   'score_limit': ('maximum score', '1000'),
                   'nbins': ('number of bins in histograms', '50')
                   }
    table_columns = [
        Column('filename_plot', TEXT),
        Column('error_msg', TEXT)
    ], ['filename_plot']

    subtable_columns = {
        'statistics': ([
            Column('machine', TEXT),
            Column('files', TEXT),
            Column('error_count', TEXT),
            Column('status', TEXT),
        ], []),
        'overscore': ([
            Column('filename', TEXT),
            Column('machine', TEXT),
            Column('score', TEXT),
            Column('size', TEXT)
        ], [])
        }

    def prepareAcquisition(self):
        link = self.config['sourceurl']
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.nbins = float(self.config['nbins'])
        self.score_limit = int(self.config['score_limit'])
        # Download the file
        self.source = hf.downloadService.addDownload(link)
        # Get URL
        self.source_url = self.source.getSourceUrl()
        self.statistics_db_value_list = []
        self.overscore_db_value_list = []
        # Set up Container for subtable data

    def extractData(self):
        import matplotlib.pyplot as plt
        from matplotlib.font_manager import FontProperties
        data = {}
        data['filename_plot'] = ""
        data['error_msg'] = ""
        path = self.source.getTmpPath()
        plot_alloc = []
        plot_score = []
        plot_size = []
        plot_maint = []
        # Function to convert seconds to readable time format
        # open file
        with open(path, 'r') as f:
            # fix the JSON-File, so the file is valid
            content = f.read()
            services = json.loads(content)
        machines = services.keys()
        for machine in machines:
            filenames = services[machine].keys()
            filenames.remove('status')
            filenames.remove('file_count')
            filenames.remove('error_count')
            allocated = list(services[machine][id]['allocated'] for id in filenames)
            allocated = filter(lambda x: x >= 0, allocated)
            alloc = list(map(lambda x: round((time.time()-float(x))/(60*60), 2), allocated))
            score = list(services[machine][id]['score']for id in filenames)
            for k in xrange(len(score)):
                if score[k] is None:
                    score[k] = 0
            sizes = list(int(services[machine][id]['size']) for id in filenames)
            maintained = list(services[machine][id]['maintained']for id in filenames)
            maintained = filter(lambda x: x != 0, maintained)
            maint = list(map(lambda x: round((time.time()-float(x))/(60*60*24), 2), maintained))
            # find data with higher score than threshold in config:
            for k in xrange(len(filenames)):
                if score[k] >= self.score_limit:
                    overscore = {
                        'filename': filenames[k],
                        'score': score[k],
                        'machine': machine,
                        'size': round(float(sizes[k]/(1024*1024)), 1)
                    }
                    self.overscore_db_value_list.append(overscore)
            plot_size.append(list(map(lambda x: float(x)/(1024*1024), sizes)))
            plot_alloc.append(alloc)
            plot_score.append(score)
            plot_maint.append(maint)

        file_count = list(services[id]['file_count']for id in machines)
        status = list(services[id]['status'] for id in machines)
        error_count = list(services[id]['error_count'] for id in machines)

        for i in xrange(len(machines)):
            details_data = {'machine': machines[i]}
            details_data['files'] = file_count[i]
            details_data['error_count'] = error_count[i]
            failed = 0
            if "failed" in status[i]:
                details_data['status'] = 'data aquisition failed'
                data['status'] = 0.5
                failed += 1
            else:
                details_data['status'] = 'data aquisition successful'
            self.statistics_db_value_list.append(details_data)
        if failed == len(machines):
            data['status'] = 0
            data['error_msg'] = "No data to display!"
            print data
            return data
        if sum(file_count) == 0:
            data['status'] = 0.5
            data['error_msg'] = "No files on caches found"
            print data
            return data
        # create three simple plots
        fig = plt.figure(figsize=(self.plotsize_x, self.plotsize_y*4))
        axis = fig.add_subplot(411)
        axis_2 = fig.add_subplot(412)
        axis_3 = fig.add_subplot(413)
        axis_4 = fig.add_subplot(414)
        nbins = self.nbins
        fontLeg = FontProperties()
        fontLeg.set_size('small')
        axis.hist([plot_size[i] for i in xrange(len(machines))], nbins, histtype='bar', stacked=True)
        axis.legend(machines, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        axis.set_xlabel('FileSize in MiB')
        axis.set_ylabel('Number of Files')
        axis.set_title('File Size Distribution')
        axis_2.hist([plot_alloc[i] for i in xrange(len(machines))], nbins, histtype='bar', stacked=True, log=True)
        axis_2.legend(machines, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        axis_2.set_xlabel('Allocated since in hours')
        axis_2.set_ylabel('Number of Files')
        axis_2.set_title('Allocation Time Distribution')
        axis_3.hist([plot_maint[i] for i in xrange(len(machines))], nbins, histtype='bar', stacked=True, log=True)
        axis_3.legend(machines, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        axis_3.set_xlabel('Maintained since in days')
        axis_3.set_ylabel('Number of Files')
        axis_3.set_title('Maintain Time Distribution')
        axis_4.hist([plot_score[i] for i in xrange(len(machines))], nbins, histtype='bar', stacked=True, log=True)
        axis_4.legend(machines, loc=6, bbox_to_anchor=(0.8, 0.88), borderaxespad=0., prop = fontLeg)
        axis_4.set_xlabel('Score ')
        axis_4.set_ylabel('Number of Files')
        axis_4.set_title('Score Distribution')
        plt.tight_layout()
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + "_filesize.png"), dpi=91)
        data["filename_plot"] = self.instance_name + "_filesize.png"
        # fill subtables
        print data
        return data

    def fillSubtables(self, parent_id):
        self.subtables['statistics'].insert().execute([dict(parent_id=parent_id, **row)
                                                       for row in self.statistics_db_value_list])
        self.subtables['overscore'].insert().execute([dict(parent_id=parent_id, **row)
                                                       for row in self.overscore_db_value_list])

    # Making Subtable Data available to the html-output
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details_list = self.subtables['statistics'].select().where(
            self.subtables['statistics'].c.parent_id == self.dataset['id']).execute().fetchall()
        data["statistics"] = map(dict, details_list)
        details_list = self.subtables['overscore'].select().where(
            self.subtables['overscore'].c.parent_id == self.dataset['id']).execute().fetchall()
        data["overscore"] = map(dict, details_list)
        return data
