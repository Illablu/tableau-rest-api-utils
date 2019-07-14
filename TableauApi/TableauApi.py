import requests
import os

class TableauApi:
	def __init__(self, username = None , password = None , contentUrl = None , serverUrl = None , apiVersion = None ):
		self.username= username if username != None  else os.environ.get('TS_USERNAME')  
		self.password=password if password != None  else os.environ.get('TS_PASSWORD')  
		self.contentUrl=contentUrl  if contentUrl != None  else os.environ.get('TS_SITE') 
		self.serverUrl=serverUrl  if serverUrl != None  else os.environ.get('TS_ADDRESS') 
		self.apiVersion=apiVersion if apiVersion !=None else os.environ.get('TS_API_VERSION')

	def batch_update(self):
		print "Doing stuff"

	def __enter__(self):
		print 'Logging in'
		self.login()
		
		return self

	def __exit__(self, exc_type, exc_value, exc_traceback): 
	  	print 'Logging out'
	  	self.logout()

	def get_request_headers(self, token = None):
		headers = {
			'Content-Type': 'application/json',
			'Accept': 'application/json'
		}
		if token is not None:
			headers['X-Tableau-Auth'] = token
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

	def logout(self):
		# TODO: add docs
		# TODO: handle connection errors
		url = '{}/api/{}/auth/signout'.format(self.serverUrl, self.apiVersion)
		r = requests.post(url, headers=self.get_request_headers(self.token))
