
import hf, time
from sqlalchemy import *

class Plot(hf.module.ModuleBase):
    def __init__(self, *pargs, **kwargs):
        hf.module.ModuleBase.__init__(self, *pargs, **kwargs)
        self.module_table = plot_table
    
    def prepareAcquisition(self, run):
        url = self.config["plot_url"]
        use_start_end_time = False
        try:
            use_start_end_time = self.config["use_start_end_time"] == "True"
        except KeyError, e:
            pass
        if use_start_end_time:
            try:
                url += "&"+self.config["starttime_parameter_name"]+"="+str(int(time.time())-int(self.config["timerange_seconds"]))
            except KeyError, e:
                pass
            try:
                url += "&"+self.config["endtime_parameter_name"]+"="+str(int(time.time()))
            except KeyError, e:
                pass
            
        self.plot = hf.downloadService.DownloadFile(url)
        
    def extractData(self):
        if self.plot.isDownloaded():
            self.plot.copyToArchive(self.instance_name+".jpg")
            self.status = 1.0
            return {"plot_file": self.plot.getFilename(), "source_url": self.plot.getSourceUrl()}
        else:
            self.status = 0.0
            return {"plot_file": ""}

plot_table = hf.module.generateModuleTable("plot", [
        Column("plot_file", TEXT)
        ])

hf.module.addColumnFileReference("plot", "plot_file")
hf.module.addModuleClass(Plot)