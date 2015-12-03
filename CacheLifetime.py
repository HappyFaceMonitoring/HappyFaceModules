# Module for Status of CMS6 via condor_q

import hf
from sqlalchemy import *
import json


class CacheLifetime(hf.module.ModuleBase):
    config_keys = {'sourceurl': ('Source Url', ''),
                   'plotsize_x': ('size of the plot in x', '10'),
                   'plotsize_y': ('size of plot in y', '5'),
                   'nbins': ('number of bins in histograms', '200')
                   }
    table_columns = [
        Column('filename_plot', TEXT),
        Column('error_msg', TEXT)
    ], ['filename_plot']

    def prepareAcquisition(self):
        link = self.config['sourceurl']
        self.plotsize_x = float(self.config['plotsize_x'])
        self.plotsize_y = float(self.config['plotsize_y'])
        self.nbins = float(self.config['nbins'])
        # Download the file
        self.source = hf.downloadService.addDownload(link)
        # Get URL
        self.source_url = self.source.getSourceUrl()
        # Set up Container for subtable data

    def extractData(self):
        import matplotlib.pyplot as plt
        from matplotlib.font_manager import FontProperties
        data = {}
        data['filename_plot'] = ""
        data['error_msg'] = ""
        path = self.source.getTmpPath()
        # Function to convert seconds to readable time format
        # open file
        with open(path, 'r') as f:
            # fix the JSON-File, so the file is valid
            content = f.read()
            services = json.loads(content)
        if services['error'] != "":
            data['status'] = 0
            data['error_msg'] = "Connection to Coordinator failed"
            return data
        jobs_id = services['life_time'].keys()
        time_list = list(int(services['life_time'][id]['time']) for id in jobs_id)
        lifetime_list = list(int(services['life_time'][id]['life_time']) for id in jobs_id)
        lifetime_list = map(lambda x: x/(60*60), lifetime_list)
        # TODO if data gets newer, constrain dataset von data from last 7 days etc.
        fig = plt.figure(figsize=(self.plotsize_x, self.plotsize_y))
        axis = fig.add_subplot(111)
        nbins = self.nbins
        fontLeg = FontProperties()
        fontLeg.set_size('small')
        axis.hist(lifetime_list, nbins, histtype='bar', log=True)
        axis.set_xlabel('Lifetime in hours')
        axis.set_ylabel('Number of Files')
        axis.set_title('Lifetime of Files in Cache')
        plt.tight_layout()
        fig.savefig(hf.downloadService.getArchivePath(
            self.run, self.instance_name + ".png"), dpi=91)
        data["filename_plot"] = self.instance_name + ".png"
        print data
        return data
