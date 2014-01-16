# Module Definition
import hf, logging
from sqlalchemy import *

class CREAMCE(hf.module.ModuleBase):
	config_keys = {
		'database': ('CREAMDB URL', 'mysql://user:password@host/creamdb'),
		'running': ('Job states corresponding to jobs which are not finished and did not fail', '0,1,2,3,4,6'),
		'node_job_status': ('Job states which should be included in the node summary','3,4')
	}

	table_columns = [],[]

	subtable_columns = {
		'status': ([
			Column('type', TEXT),
			Column('name', TEXT),
			Column('local_name', TEXT),
			Column('status', INT),
			Column('count', INT)
		], []),

		'job': ([
			Column('job_id', TEXT),
			Column('lrmsJobId', TEXT),
			Column('node', TEXT),
			Column('status', INT),
			Column('time_stamp', TIMESTAMP)

		], []),

		'type_description': ([
			Column('type', INT),
			Column('name',TEXT)
		], []),
	}

	def prepareAcquisition(self):
		#Fixme?
		self.source_url = " "

		self.queue_db_values = {}
		self.user_db_values = {}
		self.node_db_values = {}
		self.type_db_values = {}
		self.job_db_values = {}

	def getJobSummary(self,primaryColumn, secondaryColumn=None):
		self.logger.info("Generating summary for %s" % primaryColumn)
		job=self.job
		job_status=self.job_status
		running = self.config['running'].split(',')

		reload_interval = hf.config.get('happyface', 'reload_interval')
		columns = [primaryColumn, job_status.c.type, func.count('*')]
		if not secondaryColumn is None:
			columns.append(secondaryColumn)

		max = func.max(job_status.c.id).label('m')
		s1 = select([max]).group_by(job_status.c.jobId).alias('a')
		s2 = or_(job_status.c.type.in_(running), and_(not_(job_status.c.type.in_(running)),text('time_stamp > CURRENT_TIMESTAMP - INTERVAL %s MINUTE' % reload_interval)))
		s3 = select(columns).select_from(s1.join(job_status,job_status.c.id==text('m')).join(job)).where(s2).group_by(primaryColumn, job_status.c.type)

		return s3.execute().fetchall()

	def extractData(self):
		self.logger.info('Connecting to database')
		engine = create_engine(self.config['database'])
		meta = MetaData()
		meta.bind = engine
		self.job = Table('job', meta, autoload=True)
		self.job_status = Table('job_status', meta, autoload=True)
		self.job_status_type_description = Table('job_status_type_description', meta, autoload=True)

		job=self.job
		job_status=self.job_status

		s=self.job_status_type_description.select()
		self.type_db_values = s.execute().fetchall()

		self.queue_db_values = self.getJobSummary(job.c.queue)
		self.user_db_values = self.getJobSummary(job.c.userId, job.c.localUser)
		self.node_db_values = self.getJobSummary(job.c.workerNode)

		self.logger.info('Generating job list')
		node_job_status = self.config['node_job_status'].split(',')

		max = func.max(self.job_status.c.id).label('m')
		s1 = select([max]).group_by(job_status.c.jobId).alias('a')
		s2 = select([job.c.id, job.c.lrmsAbsLayerJobId, job.c.workerNode, job_status.c.type, job_status.c.time_stamp]).select_from(job.join(job_status).join(s1,job_status.c.id==text('m'))).where(and_(job_status.c.type.in_(node_job_status)))

		self.job_db_values = s2.execute().fetchall()

		return {}


	def fillSubtables(self, parent_id):
		self.subtables['status'].insert().execute([dict(parent_id=parent_id, type='queue', name=row[0], status=row[1], count=row[2], local_name=None) for row in self.queue_db_values])
		self.subtables['status'].insert().execute([dict(parent_id=parent_id, type='user', name=row[0], status=row[1], count=row[2], local_name=row[3]) for row in self.user_db_values])
		self.subtables['status'].insert().execute([dict(parent_id=parent_id, type='node', name=row[0], status=row[1], count=row[2], local_name=None) for row in self.node_db_values])

		self.subtables['type_description'].insert().execute([dict(parent_id=parent_id, **row) for row in self.type_db_values])
		self.subtables['job'].insert().execute([dict(parent_id=parent_id, job_id=row[0], lrmsJobId=row[1], node=row[2], status=row[3], time_stamp=row[4]) for row in self.job_db_values])


	def getTemplateJobData(self,type):
		info_list = self.subtables['status'].select().where(and_(self.subtables['status'].c.parent_id==self.dataset['id'],self.subtables['status'].c.type==type)).execute().fetchall()

		data = {None: {'total':0, 'local_name':''}}

		for i in info_list:
			if not i['name'] in data:
				data[i['name']]={'total':0}

			data[i['name']][i['status']]=i['count']
			data[i['name']]['local_name']=i['local_name']
			data[i['name']]['total']+=i['count']

			data[None][i['status']] = data[None].get(i['status'],0)+i['count']
			data[None]['total']+=i['count']

		return data


	def getTemplateData(self):
		data = hf.module.ModuleBase.getTemplateData(self)

		data['queue_list'] = self.getTemplateJobData('queue')
		data['user_list'] = self.getTemplateJobData('user')
		data['node_list'] = self.getTemplateJobData('node')

		data['type_list'] = self.subtables['type_description'].select().where(self.subtables['type_description'].c.parent_id==self.dataset['id']).execute().fetchall()

		nodes = select([self.subtables['job'].c.node]).where(self.subtables['job'].c.parent_id==self.dataset['id']).distinct().execute().fetchall()

		node_job_status = map(int,self.config['node_job_status'].split(','))


		jobs = self.subtables['job'].select().where(self.subtables['job'].c.parent_id==self.dataset['id']).execute().fetchall()

		J={}

		for j in jobs:
			if not j['node'] in J:
				J[j['node']]={}
			if not j['status'] in J[j['node']]:
				J[j['node']][j['status']]=[]
			J[j['node']][j['status']].append(j)

		data['job_list'] = J

		return data
