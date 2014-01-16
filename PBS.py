import hf
from sqlalchemy import *
from random import random
import datetime

class PBS(hf.module.ModuleBase):
	config_keys = {
		'qstat_log_url': ('qstat log file', '')
	}

	table_columns = [
		Column('url', TEXT),
	], []

	subtable_columns = {
		'status': ([
			Column('type', TEXT),
			Column('name', TEXT),
			Column('status', TEXT),
			Column('count', INT)
		], []),

		'total': ([
			Column('type', TEXT),
			Column('count',INT)
		], []),
	}

	def prepareAcquisition(self):
		self.source = hf.downloadService.addDownload(self.config['source_url'])
		self.source_url = self.source.getSourceUrl()

		self.summary={}
		self.type_db_values = [{'type':'Q', 'name':'Queued'}, {'type':'R', 'name':'Running'}, {'type':'H', 'name':'Held'}]

	def extractJobs(self):
		with open(self.source.getTmpPath(), 'r') as f:
			jobs = {}
			Id = ""
			Key = ""
			for line in f:
				sline=line.lstrip()
				depth=(len(line)-len(sline))/4
				sline=sline.rstrip()

				if line[0]=='\t':
					jobs[Id][Key]+=sline
				elif depth==0:
					k=sline.split(": ", 2)
					if k[0]=="Job Id":
						Id = k[1]
						jobs[Id] = {}
				elif depth==1:
					k, val=sline.split(" = ", 2)
					Key = k
					jobs[Id][Key] = val

		return jobs

	def getJobSummary(self,jobs,keys):
		summary = {}
		self.total = {'Q':0, 'R':0, 'H':0}

		for k in keys:
			summary[k]={}

		for j in jobs:

			state = jobs[j]['job_state']

			for k in keys:
				name = jobs[j][k].split('@')[0]

				if not name in summary[k]:
					summary[k][name]={}

				if not state in summary[k][name]:
					summary[k][name][state]=0

				summary[k][name][state]+=1

			if not state in self.total:
				self.total[state]=0

			self.total[state]+=1

		return summary

	def extractData(self):
		jobs = self.extractJobs()
		self.summary = self.getJobSummary(jobs,['queue','Job_Owner'])

		end = datetime.datetime.now()
		begin = end - datetime.timedelta(hours=24)

		keys = {}

		keys['start_date']=begin.strftime("%Y-%m-%d")
		keys['end_date']=end.strftime("%Y-%m-%d")
		keys['start_time']=begin.strftime("%H:%M")
		keys['end_time']=end.strftime("%H:%M")
		keys['title']=self.instance_name
		keys['legend']='2'

		i = 1
		for t in self.type_db_values:
			keys["filter_%d" % i] = "type,%s" % t['type']
			keys["curve_%d" % i] = "%s,total,count,%s" % (self.instance_name, t['name'])
			i+=1

		url = hf.plotgenerator.getTimeseriesUrl() + 'img?'

		for k in keys:
			url += k + '=' + keys[k] + '&'

		return {'url':url}

	def fillSubtables(self, parent_id):
		queue = []
		user = []

		for q in self.summary['queue']:
			for i in self.summary['queue'][q]:
				queue.append(dict(parent_id=parent_id, type='queue', name=q, status=i, count=self.summary['queue'][q][i]))

		for u in self.summary['Job_Owner']:
			for i in self.summary['Job_Owner'][u]:
				queue.append(dict(parent_id=parent_id, type='user', name=u, status=i, count=self.summary['Job_Owner'][u][i]))

		self.subtables['status'].insert().execute(queue)
		self.subtables['status'].insert().execute(user)

		self.subtables['total'].insert().execute([dict(parent_id=parent_id, type=row, count=self.total[row]) for row in self.total])

	def getTemplateJobData(self,type):
		info_list = self.subtables['status'].select().where(and_(self.subtables['status'].c.parent_id==self.dataset['id'],self.subtables['status'].c.type==type)).execute().fetchall()

		data = {None: {'total':0}}

		for i in info_list:
			if not i['name'] in data:
				data[i['name']]={'total':0}

			data[i['name']][i['status']]=i['count']
			data[i['name']]['total']+=i['count']

			data[None][i['status']] = data[None].get(i['status'],0)+i['count']
			data[None]['total']+=i['count']

		return data

	def getTemplateData(self):
		data = hf.module.ModuleBase.getTemplateData(self)

		data['queue_list'] = self.getTemplateJobData('queue')
		data['user_list'] = self.getTemplateJobData('user')
		data['type_list'] = [{'type':'Q', 'name':'Queued'}, {'type':'R', 'name':'Running'}, {'type':'H', 'name':'Held'}]

		return data
