import logging
import os
import requests


class TableauApi:
    def __init__(
        self,
        username=None,
        password=None,
        contentUrl=None,
        serverUrl=None,
        apiVersion=None
    ):
        self.username = username \
            if username is not None \
            else os.environ.get('TS_USERNAME')
        self.password = password \
            if password is not None \
            else os.environ.get('TS_PASSWORD')
        self.contentUrl = contentUrl \
            if contentUrl is not None \
            else os.environ.get('TS_SITE')
        self.serverUrl = serverUrl \
            if serverUrl is not None \
            else os.environ.get('TS_ADDRESS')
        self.apiVersion = apiVersion \
            if apiVersion is not None \
            else os.environ.get('TS_API_VERSION')
        self.token = None
        self.site = None

    def batch_update(self, newuser, newpass):
        """Updates login info to all public and per-project datasources"""
        self.datasources = []

        self.get_public_datasources()
        self.get_project_datasources()

        logging.info('DATASOURCES IDS')
        logging.info(list(map(lambda x: x['datasource_id'], self.datasources)))

        for datasource in self.datasources:
            update_url = \
               '{}/api/{}/sites/{}/datasources/{}/connections/{}'.format(
                    self.serverUrl,
                    self.apiVersion,
                    self.site,
                    datasource["datasource_id"],
                    datasource['connection_id']
                )
            payload = {
                'connection': {
                    'userName': newuser,
                    'password': newpass,
                    'embedPassword': True
                }
            }
            response = self.update_in_tableau_api(update_url, payload)
            logging.info('Updating data source credentials for {}{}'.format(
                datasource['datasource_name'],
                datasource['datasource_id'])
            )

            logging.debug(response.json())

            # TODO: handle errors

    def update_in_tableau_api(self, update_url, payload):
        """Pushes changes update against Tableau's API"""
        return requests.put(
            update_url,
            headers=self.get_request_headers(),
            json=payload
        )

    def __enter__(self):
        """TODO: some class/method description"""
        logging.info('Logging in')
        self.login()

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """TODO: some class/method description"""
        logging.info('Logging out')
        self.logout()

    def get_request_headers(self):
        """TODO: some class/method description"""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        if self.token is not None:
            headers['X-Tableau-Auth'] = self.token

        return headers

    def login(self):
        """TODO: some class/method description"""
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
        response = requests.post(
                url,
                headers=self.get_request_headers(),
                json=payload
            )

        # TODO:add error handling
        self.token = response.json()['credentials']['token']
        self.site = response.json()['credentials']['site']['id']

    def logout(self):
        """TODO: some class/method description"""
        # TODO: handle connection errors
        url = '{}/api/{}/auth/signout'.format(self.serverUrl, self.apiVersion)
        requests.post(url, headers=self.get_request_headers())

    def get_public_datasources(self):
        """TODO: some class/method description"""
        datasources_url = '{}/api/{}/sites/{}/datasources'.format(
            self.serverUrl,
            self.apiVersion,
            self.site
        )

        response = requests.get(
                datasources_url,
                headers=self.get_request_headers()
            )

        if 'datasource' in response.json()['datasources']:
            logging.info("getting connections:")
            sources = response.json()['datasources']['datasource']
            for source in sources:
                connections_url = \
                    '{}/api/{}/sites/{}/datasources/{}/connections'.format(
                        self.serverUrl,
                        self.apiVersion,
                        self.site,
                        source['id']
                    )
                response = requests.get(
                    connections_url,
                    headers=self.get_request_headers()
                )
                if 'connection' in response.json()['connections']:
                    connections = response.json()['connections']['connection']
                    for connection in connections:
                        logging.info(
                            "datasource: {}({}), type: {}, connection id: {}"
                            "connection username: {}".format(
                                source['name'],
                                source['id'],
                                connection['type'],
                                connection['id'],
                                connection['userName']
                                )
                            )

                        self.datasources.append({
                            'datasource_id': source['id'],
                            'datasource_name': source['name'],
                            'datasource_content_url': source['contentUrl'],
                            'connection_id': connection['id'],
                            'connection_type': connection['type'],
                            'connection_server_address':
                                connection['serverAddress'],
                            'connection_server_port': connection['serverPort'],
                            'connection_username': connection['userName'],
                        })

    def get_project_datasources(self):
        """TODO: some class/method description"""
        workbooks_url = '{}/api/{}/sites/{}/workbooks?pageSize=1000'.format(
            self.serverUrl,
            self.apiVersion,
            self.site
        )

        response = requests.get(
            workbooks_url,
            headers=self.get_request_headers()
        )

        if 'workbook' in response.json()['workbooks']:
            workbooks = response.json()['workbooks']['workbook']

            for workbook in workbooks:
                logging.info("getting workbook connections:")
                connections_url = \
                    '{}/api/{}/sites/{}/workbooks/{}/connections'.format(
                        self.serverUrl,
                        self.apiVersion,
                        self.site, workbook['id']
                    )
                response = requests.get(
                        connections_url,
                        headers=self.get_request_headers()
                    )
                if 'connection' in response.json()['connections']:
                    connections = response.json()['connections']['connection']
                    for connection in connections:
                        logging.info(
                            "workbook: {}, datasource: {}({}), type: {}, "
                            "connection id: {} connection username: {}".format(
                                workbook['name'],
                                connection['datasource']['name'],
                                connection['datasource']['id'],
                                connection['type'],
                                connection['id'],
                                connection['userName']
                            )
                        )
                        self.datasources.append({
                            'datasource_id': connection['datasource']['id'],
                            'datasource_name':
                                connection['datasource']['name'],
                            'datasource_content_url': workbook['contentUrl'],
                            'connection_id': connection['id'],
                            'connection_type': connection['type'],
                            'connection_server_address':
                                connection['serverAddress'],
                            'connection_server_port': connection['serverPort'],
                            'connection_username': connection['userName'],
                        })
