# -*- coding: utf-8 -*-

import json, time
import logging
import os
from sqlalchemy import Column, TEXT

import hf
import htcondor

class BatchCpuEffHistory(hf.module.ModuleBase):
    config_keys = {'source_url': ('Not used, but filled to avoid warnings', 'www.google.com'),
                   'htcondor_collector': ('The htcondor collector instance', 'ekpcondorcentral.ekp.kit.edu')}
    table_columns = [
                      Column("filename_plot", TEXT)
                    ], ["filename_plot"]

    def prepareAcquisition(self):
        self.condor_job_status = {0: 'unexpanded',
                             1: 'idle',
                             2: 'running',
                             3: 'removed',                                                                                       4: 'completed',
                             5: 'held',
                             6: 'submission_er',
                             7: 'suspended'}
        self.eff_history_fn = '/ekpcommon/happyface/upload/ekplocal/batch_efficiency.history'
        self.logger = logging.getLogger(__name__)
        self.source_url = self.config['htcondor_collector']


    def extractData(self):
	data = {}
        history_producer = self.produce_history()
        history_producer.next()
        self.logger.info('Processing efficiency information')
        for job in self.get_htcondor_information(self.config['htcondor_collector']):
            if self.condor_job_status[job[u'JobStatus']] in ('running',) and job[u'JobUniverse'] != 9:
                history_producer.send(job)
        data['filename_plot'] = self.plot()
        return data

    def produce_history(self):
        eff_history = self.load_eff_history(self.eff_history_fn) or {}
        try:
            while True:
                next_job = (yield)
                next_job[u'JobStatus'] = self.condor_job_status[next_job[u'JobStatus']]
                next_job[u'RemoteTotalCpu'] = next_job[u'RemoteSysCpu'] + next_job[u'RemoteUserCpu']
                if next_job[u'JobStatus'] in ('running', 'removed', 'completed', 'held'):
                    next_job[u'RemoteWalltime'] = (next_job[u'ServerTime']
                                                    - next_job[u'JobStartDate'])
                    try:
                        next_job[u'CpuEfficiency'] = (round(next_job[u'RemoteTotalCpu']
                                                        / float(next_job[u'RequestCpus'] 
                                                        * next_job[u'RemoteWalltime']), 2))
                    except ZeroDivisionError:
                        pass
                try:
                    next_job[u'CumulatedIO'] = next_job[u'BytesRecvd'] + next_job[u'BytesSent']
                except KeyError:
                    next_job[u'CumulatedIO'] = 0.0
                job_history = eff_history.setdefault(next_job['GlobalJobId'], dict())
                job_history['last'] = time.time()
                job_history['ncpus'] = next_job['RequestCpus']
                job_history[int(next_job['RemoteWalltime'])] = (next_job.get('RemoteTotalCpu', 0),
                                                next_job.get('CumulatedIO', 0))
        except GeneratorExit:
            self.logger.info('Writing efficiency history')
            with open(self.eff_history_fn, 'w') as f:
                json.dump(eff_history, f)

    def load_eff_history(self, eff_history_fn):
        self.logger.info('Loading efficiency history')
        eff_history = {}
        if os.path.exists(eff_history_fn):
            try:
                eff_history = json.load(open(eff_history_fn))
            except ValueError:
                os.unlink(eff_history_fn)
                raise
            for job_id in list(eff_history):  # forget about jobs after 15min
                if time.time() - eff_history[job_id].get('last', 0) > 15 * 60:
                    eff_history.pop(job_id)
            return eff_history
    
    def get_htcondor_information(self, htcondor_collector_host):
        htcondor_collector = htcondor.Collector(htcondor_collector_host)
        htcondor_schedds_ads = htcondor_collector.locateAll(htcondor.DaemonTypes.Schedd)
    
        for htcondor_schedd_ad in htcondor_schedds_ads:
            htcondor_schedd = htcondor.Schedd(htcondor_schedd_ad)
            try:
                htcondor_jobs = htcondor_schedd.query()
            except IOError: # Some schedd like gridka26.gridka.de do not return jobs, instead IOError is thrown
                pass
            else:
                for htcondor_job in htcondor_jobs:
                    yield dict(htcondor_job.items())
    
    
    def plot(self):
        import matplotlib
        matplotlib.use("agg")
        import matplotlib.pyplot
        
        eff_history = json.load(open('/ekpcommon/happyface/upload/ekplocal/batch_efficiency.history'))

        plot_color = {}
        plot_color2 = {}
        plot_data_x = {}
        plot_data_x2 = {}
        plot_data_y = {}
        plot_data_y2 = {}
        max_x = 2*24*60*60
        lastTime_all = 0
        for jobid in eff_history:
                lastTime = int(eff_history[jobid].pop('last'))
                lastTime_all = max(lastTime_all, lastTime)
                if lastTime < time.time() - 15*60:
                        continue
                ncpu = eff_history[jobid].pop('ncpus', 1)
                if ncpu == 1:
                        plot_color[jobid] = 'r'
                        plot_color2[jobid] = 'g'
                else:
                        plot_color[jobid] = 'b'
                        plot_color2[jobid] = 'g'
                eff_history_tmp = {}
                for walltime_str in eff_history[jobid]:
                        tmp = eff_history[jobid][walltime_str]
                        eff_history_tmp[int(walltime_str)] = tmp

                eff_history_x = plot_data_x.setdefault(jobid, [])
                eff_history_x2 = plot_data_x2.setdefault(jobid, [])
                eff_history_y = plot_data_y.setdefault(jobid, [])
                eff_history_y2 = plot_data_y2.setdefault(jobid, [])
                for walltime in sorted(eff_history_tmp):
                        if isinstance(eff_history_tmp[walltime], int):
                                cputime = int(eff_history_tmp[walltime])
                                io = 0
                        else:
                                cputime = int(eff_history_tmp[walltime][0])
                                io = float(eff_history_tmp[walltime][1])

                        eff_history_x.append(walltime)
                        max_x = max(max_x, walltime)
                        eff_history_y.append(100. * cputime / float(max(1, walltime) * ncpu))

                        eff_history_x2.append(walltime)
                        eff_history_y2.append(io / ncpu)

        fig = matplotlib.pyplot.figure()
        ax = fig.add_subplot(111, ylim=(0,102))
        ax.set_xscale('log')
        ax.set_xlim((60, max_x))
        ax.set_xlabel('Walltime')

        # custom tick labels for x axis in min, h and days
        tickloc = [1, 2, 3, 4, 5, 6,7,8,9, 10, 20, 30, 40, 50, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 660, 720, 1440, 2880]
        ax.set_xticks([60 * i for i in tickloc])
        ax.set_xticklabels(["1 min", 2, 3, 4, 5, '', '', '', '', 10, 20, 30, '', '', "1 h", 2, 3, 4, '', 6, '', 8, '', '', '', 12, "1 d", 2])
        ax.set_ylabel('Accumulated CPU efficiency [%]', color = 'r')

        ax2 = ax.twinx()
        ax2.set_xlim((60, max_x))
        ax2.set_ylim((1e-3, 1e5))
        ax2.minorticks_off()
        ax2.set_ylabel('Accumulated IO [arb.]', color = 'g')
        ax2.set_yscale('log')

        for jobid in plot_data_x:
                if not plot_data_x2[jobid]:
                        continue
                if plot_data_y[jobid]:
                        ax.plot(plot_data_x[jobid], plot_data_y[jobid], drawstyle = '-',
                                linewidth = 2, color = plot_color[jobid], alpha = 0.03)
                if filter(lambda x: x <= 0, plot_data_y2[jobid]):
                        continue
                ax2.plot(plot_data_x2[jobid], plot_data_y2[jobid], drawstyle = '-',
                        linewidth = 2, color = plot_color2[jobid], alpha = 0.03)

        lastTime_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(lastTime_all))
        ax.text(0, 1.05, 'Individual job history (last update: %s)' % lastTime_str, transform = ax.transAxes)
        #print lastTime_str
        plotname = hf.downloadService.getArchivePath( self.run, self.instance_name + ".png")
        fig.savefig(plotname, dpi=90, bbox_inches='tight')
        return plotname

def coroutine(generator):
            def start_coroutine(*args, **kwargs):
                started_generator = generator(*args, **kwargs)
                next(started_generator)
                return started_generator
            return start_coroutine
