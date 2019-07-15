import requests
import os

class TableauApi:
	def __init__(self, username = None , password = None , contentUrl = None , serverUrl = None , apiVersion = None ):
		self.username= username if username != None  else os.environ.get('TS_USERNAME')  
		self.password=password if password != None  else os.environ.get('TS_PASSWORD')  
		self.contentUrl=contentUrl  if contentUrl != None  else os.environ.get('TS_SITE') 
		self.serverUrl=serverUrl  if serverUrl != None  else os.environ.get('TS_ADDRESS') 
		self.apiVersion=apiVersion if apiVersion !=None else os.environ.get('TS_API_VERSION')
		self.token = None
		self.site = None

	def batch_update(self, newuser, newpass):
		self.datasources = []
	
		self.get_public_datasources()
		self.get_project_datasources()

		print "----------------------------------------"
		print 'DATASOURCES IDS'
		print  list(map(lambda x : x['datasource_id'], self.datasources))

		for datasource in self.datasources:
			update_url = '{}/api/{}/sites/{}/datasources/{}/connections/{}'.format(self.serverUrl, self.apiVersion, self.site, datasource["datasource_id"], datasource['connection_id'])
			payload = {
				'connection': {
					'userName': newuser,
					'password': newpass,
					'embedPassword': True
					}
				}
			r = requests.put(update_url, headers=self.get_request_headers(), json=payload)
			print 'Updating data source credentials for {}{}'.format(datasource['datasource_name'],datasource['datasource_id'])
			#print  r.json()
			# TODO: handle errors

	def __enter__(self):
		print 'Logging in'
		self.login()
		
		return self

	def __exit__(self, exc_type, exc_value, exc_traceback): 
	  	print 'Logging out'
	  	self.logout()

	def get_request_headers(self):
		headers = {
			'Content-Type': 'application/json',
			'Accept': 'application/json'
		}

		if self.token is not None:
			headers['X-Tableau-Auth'] = self.token
		return headers

	def login(self):
		# TODO: add docs
		# TODO: handle connection errors
		payload = {
			'credentials': {
				'name': self.username,
				'password': self.password,
				'site': {
					'contentUrl': self.contentUrl
					}
				}
			}

		url = '{}/api/{}/auth/signin'.format(self.serverUrl, self.apiVersion)
		r = requests.post(url, headers=self.get_request_headers(), json=payload);
		#will add error
		self.token = r.json()['credentials']['token']
		self.site = r.json()['credentials']['site']['id']

	def logout(self):
		# TODO: add docs
		# TODO: handle connection errors
		url = '{}/api/{}/auth/signout'.format(self.serverUrl, self.apiVersion)
		r = requests.post(url, headers=self.get_request_headers())

	def get_public_datasources(self):
		site_token = self.token
		datasources_url = '{}/api/{}/sites/{}/datasources'.format(self.serverUrl, self.apiVersion, self.site)

		r = requests.get(datasources_url, headers=self.get_request_headers())

		if 'datasource' in r.json()['datasources']:
			print "getting connections:"
			sources = r.json()['datasources']['datasource']
			for source in sources:
				connections_url = '{}/api/{}/sites/{}/datasources/{}/connections'.format(self.serverUrl, self.apiVersion, self.site, source['id'])
				r = requests.get(connections_url, headers=self.get_request_headers())
				if 'connection' in r.json()['connections']:
					connections = r.json()['connections']['connection']
					for connection in connections:
						print "datasource: {}({}), type: {}, connection id: {} connection username: {}".format(source['name'], source['id'], connection['type'], connection['id'],  connection['userName'], )

						self.datasources.append({
						'datasource_id': source['id'],
						'datasource_name': source['name'],
						'datasource_content_url': source['contentUrl'],
						'connection_id': connection['id'],
						'connection_type': connection['type'],
						'connection_server_address': connection['serverAddress'],
						'connection_server_port': connection['serverPort'],
						'connection_username': connection['userName'],
						})

	def get_project_datasources(self):
		site_token = self.token
		workbooks_url = '{}/api/{}/sites/{}/workbooks?pageSize=1000'.format(self.serverUrl, self.apiVersion, self.site)

		r = requests.get(workbooks_url, headers=self.get_request_headers())

		if 'workbook' in r.json()['workbooks']:
			workbooks = r.json()['workbooks']['workbook']

			for workbook in workbooks:
				print "getting workbook connections:"
				connections_url = '{}/api/{}/sites/{}/workbooks/{}/connections'.format(self.serverUrl, self.apiVersion, self.site, workbook['id'])
				r = requests.get(connections_url, headers=self.get_request_headers())
				if 'connection' in r.json()['connections']:
					connections = r.json()['connections']['connection']
					for connection in connections:
						print "workbook: {}, datasource: {}({}), type: {}, connection id: {} connection username: {}".format(workbook['name'],connection['datasource']['name'], connection['datasource']['id'], connection['type'], connection['id'],  connection['userName'], )
						self.datasources.append({
						'datasource_id': connection['datasource']['id'],
						'datasource_name':connection['datasource']['name'],
						'datasource_content_url': workbook['contentUrl'],
						'connection_id': connection['id'],
						'connection_type': connection['type'],
						'connection_server_address': connection['serverAddress'],
						#'connection_server_port': connection['serverPort'],
						'connection_username': connection['userName'],
						})