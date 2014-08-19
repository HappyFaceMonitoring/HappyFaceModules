#!/usr/bin/env python

import hf, logging, json, time
from sqlalchemy import *


class Batch_eff_plots(hf.module.ModuleBase):
    config_keys = {
        'jason_history': ('URL of the jason file', '')
    }

    table_columns = [
        Column("filename_eff_plot", TEXT),
        Column("filename_eff_inst_plot", TEXT)
    ], ['filename_eff_plot', 'filename_eff_inst_plot']

    
    def prepareAcquisition(self):

        if 'jason_history' not in self.config: raise hf.exceptions.ConfigError('jason_history option not set')
        self.jason_history = hf.downloadService.addDownload(self.config['jason_history'])
        self.source_url = self.jason_history.getSourceUrl()
    def extractData(self):
    	import matplotlib
	matplotlib.use("agg")
	import matplotlib.pyplot
	
	data = {}
        data["filename_eff_plot"] = ""
        data["filename_eff_inst_plot"] = ""
        data['status'] = 1.0
	
	eff_history = json.load(open(self.jason_history.getTmpPath()))

	plot_color = {}
	plot_color2 = {}
	plot_data_x = {}
	plot_data_x2 = {}
	plot_data_y = {}
	plot_data_y2 = {}
	plot_color3 = {}
	plot_data_x3 = {}
	plot_data_y3 = {}
	max_x = 61
	max_x2 = 61
	lastTime_all = 0
	for jobid in eff_history:
	    lastTime = eff_history[jobid].pop('last')
	    lastTime_all = max(lastTime_all, lastTime)
	    if lastTime < time.time() - 15*60:
		continue
	    ncpu = eff_history[jobid].pop('ncpus', 1)
	    if ncpu == 1:
		plot_color[jobid] = 'r'
		plot_color2[jobid] = 'g'
		plot_color3[jobid] = 'r'
	    else:
		plot_color[jobid] = 'b'
		plot_color2[jobid] = 'g'
		plot_color3[jobid] = 'b'
	    eff_history_tmp = {}
	    eff_history_tmp2 = {}
	    for walltime_str in eff_history[jobid]:
		tmp = eff_history[jobid][walltime_str]
		eff_history_tmp[int(walltime_str)] = tmp
		if isinstance(tmp, int):
		    eff_history_tmp2[int(walltime_str)] = int(tmp)
		else:
		    eff_history_tmp2[int(walltime_str)] = int(tmp[0])

	    eff_history_x = plot_data_x.setdefault(jobid, [])
	    eff_history_x2 = plot_data_x2.setdefault(jobid, [])
	    eff_history_x3 = plot_data_x3.setdefault(jobid, [])
	    eff_history_y = plot_data_y.setdefault(jobid, [])
	    eff_history_y2 = plot_data_y2.setdefault(jobid, [])
	    eff_history_y3 = plot_data_y3.setdefault(jobid, [])
	    
	    for walltime in sorted(eff_history_tmp):
		if isinstance(eff_history_tmp[walltime], int):
		    cputime = int(eff_history_tmp[walltime])
		    io = 0
		else:
		    cputime = int(eff_history_tmp[walltime][0])
		    io = float(eff_history_tmp[walltime][1])

		eff_history_x.append(walltime / ncpu)
		max_x = max(max_x, walltime / ncpu)
		eff_history_y.append(100. * cputime / float(max(1, walltime)))

		eff_history_x2.append(walltime / ncpu)
		eff_history_y2.append(io / ncpu)
		
	    eff_history_list = sorted(eff_history_tmp2)
	    for (wt1, wt2) in zip(eff_history_list, eff_history_list[1:]):
		eff_history_x3.append(wt1 / ncpu)
		max_x2 = max(max_x2, wt2 / ncpu)
		eff_history_y3.append(100. * (eff_history_tmp2[wt2] - eff_history_tmp2[wt1]) / float(max(1, wt2-wt1)))
	fig1 = matplotlib.pyplot.figure()
	ax = fig1.add_subplot(111, ylim=(0,102))
	ax.set_xscale('log')
	ax.set_xlim((60, max_x))
	ax.set_xlabel('Walltime [s]')
	ax.set_ylabel('Accumulated CPU efficiency [%]')

	ax2 = ax.twinx()
	ax2.set_xlim((60, max_x))
	ax2.set_ylabel('Accumulated IO [arb.]')
	ax2.set_yscale('log')

	for jobid in plot_data_x:
	    ax.plot(plot_data_x[jobid], plot_data_y[jobid], drawstyle = '-',
		linewidth = 2, color = plot_color[jobid], alpha = 0.03)
	    ax2.plot(plot_data_x2[jobid], plot_data_y2[jobid], drawstyle = '-',
		linewidth = 2, color = plot_color2[jobid], alpha = 0.03)

	lastTime_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(lastTime_all))
	ax.text(0, 1.05, 'Individual job history (last update: %s)' % lastTime_str, transform = ax.transAxes)
	#print lastTime_str
	fig1.savefig(hf.downloadService.getArchivePath(self.run, self.instance_name + "_batch_efficiency.png"), dpi = 60)
	data["filename_eff_plot"] = self.instance_name + "_batch_efficiency.png"
	
	fig2 = matplotlib.pyplot.figure()
	fig2.subplots_adjust(left=0.1, right=0.95, top = 0.95, bottom = 0.1)
	ax = fig2.add_subplot(111, xlim=(60, max_x2), ylim=(-2,102))
	#ax.set_xscale('log')
	ax.set_xlabel('Walltime [s]')
	ax.set_ylabel('Instantaneous CPU efficiency [%]')
	if False:
	    for jobid in plot_data_x3:
		ax.errorbar(plot_data_x3[jobid], plot_data_y3[jobid], fmt = 'o',
		    markeredgewidth = 0, linewidth = 0, color = plot_color3[jobid],
		    alpha = 0.03, markersize = 3)
	else:
	    for color in ['b', 'r']:
		allx = []
		ally = []
		for jobid in plot_data_x3:
		    if plot_color3[jobid] != color:
			continue
		    for idx, x in enumerate(plot_data_x3[jobid]):
			    if		(plot_data_x3[jobid][idx] > 0) and \
					    (plot_data_x3[jobid][idx] < max_x2 + 2) and \
					    (plot_data_y3[jobid][idx] >= -2) and \
					    (plot_data_y3[jobid][idx] <= 102):
				    allx.append(plot_data_x3[jobid][idx])
				    ally.append(plot_data_y3[jobid][idx])
		if not allx:
		    continue
		if color == 'r':
			tmp = matplotlib.pyplot.hexbin(x = allx, y = ally,
				linewidth = 1, gridsize = 40, xscale = 'log', bins = 'log', cmap = 'Reds', mincnt=1)
			tmp.set_alpha(0.7)
		elif color == 'b':
			tmp = matplotlib.pyplot.hexbin(x = allx, y = ally,
				linewidth = 1, gridsize = 40, xscale = 'log', bins = 'log', cmap = 'Blues', mincnt=1)
			tmp.set_alpha(0.7)
		matplotlib.pyplot.colorbar(pad = 0.01, fraction = 0.1)
	fig2.savefig(hf.downloadService.getArchivePath(self.run, self.instance_name + "_batch_efficiency_inst.png"), dpi = 60)
	data["filename_eff_inst_plot"] = self.instance_name + "_batch_efficiency_inst.png"
	return data