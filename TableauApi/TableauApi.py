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
            else os.environ.get('TO_USERNAME')
        self.password = password \
            if password is not None \
            else os.environ.get('TO_PASSWORD')
        self.contentUrl = contentUrl \
            if contentUrl is not None \
            else os.environ.get('TO_SITE')
        self.serverUrl = serverUrl \
            if serverUrl is not None \
            else os.environ.get('TO_ADDRESS')
        self.apiVersion = apiVersion \
            if apiVersion is not None \
            else os.environ.get('TO_API_VERSION')
        self.token = None
        self.site = None
        self.user = None

        # Destination project in Tableau Server - RTC
        self.parent_project_id = "c8799877-344b-4628-8985-f51aeb09325e"
        self.workbook_download_path = './tmp'

    def move_projects_to_tableau_server(self):
        """Moves set of projects from Tableau Online to a target project on Tableau Server"""
        # The following method lists the projects available on Tableau Online:
        projects = self.get_t_online_projects() #take the first for debug
       
        ### Debug: get the full list of available projects on Tableau Server
        # pp = ts_api.get_t_server_projects()
        # print(pp)
        # print(len(pp))

        # A list of test project to try this on. Uncomment one to work on a subset of projects
        # Uber, has mainly twbx Workbooks:
        #projects = list(filter(lambda p: p["id"] == "cbdafd11-bbcd-4dee-8e20-e8df87e7bffc", projects))
        # Maggie, has mainly twb Workbooks:
        #projects = list(filter(lambda p: p["id"] == "064cd98f-388b-4727-b938-f5edddba4946", projects))

        # cycling each project found on Tableau Online....
        for project in projects:
            with TableauApi(
                username=os.environ.get('TS_USERNAME'),
                password=os.environ.get('TS_PASSWORD'),
                contentUrl="",
                apiVersion=os.environ.get('TS_API_VERSION'),
                serverUrl=os.environ.get('TS_SERVER_URL')
            ) as ts_api:
                # .... it lookups in Tableau Server, for a project with the same name.
                maybe_existing_project_id = ts_api.existing_project_by_name_on_t_server(project["name"])

                payload = {
                    'project': {
                        'parentProjectId': self.parent_project_id,
                        'name': project["name"],
                        'description': project["description"],
                        'contentPermissions': project["contentPermissions"]
                    }
                }

                # if a matching project exists, updates its details in Tableau Server:
                if maybe_existing_project_id != "false":
                    update_url = \
                   '{}/api/{}/sites/{}/projects/{}'.format(
                        ts_api.serverUrl,
                        ts_api.apiVersion,
                        ts_api.site,
                        maybe_existing_project_id
                    )
                    response = ts_api.update_in_tableau_api(update_url, payload)
                    ts_project_id = response.json()["project"]["id"]
                # otherwise, it creates a new one in Tableau Server:
                else:
                    insert_url = \
                   '{}/api/{}/sites/{}/projects'.format(
                        ts_api.serverUrl,
                        ts_api.apiVersion,
                        ts_api.site
                    )
                    response = ts_api.create_in_tableau_api(insert_url, payload)
                    ts_project_id = response.json()["project"]["id"]

                # Once created/updated the projects, it starts cycling its workbooks
                workbooks_url = '{}/api/{}/sites/{}/workbooks?pageSize=1000'.format(
                    self.serverUrl,
                    self.apiVersion,
                    self.site
                )

                response = requests.get(
                    workbooks_url,
                    headers=self.get_request_headers()
                )

                # There's no endpoint for fetching/filtering workbooks per-project
                # We download the full list of workbooks....
                if 'workbook' in response.json()['workbooks']:
                    workbooks = response.json()['workbooks']['workbook']

                    # ...then filter the list by the current project
                    workbooks_per_project = filter(lambda p: p["project"]["id"] == project["id"], workbooks)

                    # For each workbook of the current project....
                    for workbook in list(workbooks_per_project):
                        # its downloads Workbook from Tableau Online...
                        workbook_url = '{}/api/{}/sites/{}/workbooks/{}/content'.format(
                            self.serverUrl,
                            self.apiVersion,
                            self.site,
                            workbook["id"]
                        )

                        print("---")
                        print("downloading")
                        print(workbook["name"])
                        print("---")
                        
                        r = requests.get(workbook_url, headers=self.get_request_headers(), allow_redirects=True)

                        # (file format depends on the content-type of the response)
                        file_format = 'twbx' if r.headers["Content-Type"] == "application/octet-stream" else "twb"
                        workbook_file_name = project["name"] + "---" + workbook["name"] + "." + file_format

                        # ...saves it locally (tmp/ folder) with a sensible naming
                        open(self.workbook_download_path + '/' + workbook_file_name, 'wb').write(r.content)
                        
                        ### DEBUG: skipping upload, as it crashes.
                        continue

                        # upload workbook to tableau server
                        #POST /api/api-version/sites/site-id/fileUploads
                        #PUT /api/api-version/sites/site-id/fileUploads/upload-session-id
                        #POST /api/api-version/sites/site-id/workbooks

                        publish_workbook_url = \
                        '{}/api/{}/sites/{}/workbooks?overwrite=true&asJob=true&workbookType={}'.format(
                            ts_api.serverUrl,
                            ts_api.apiVersion,
                            ts_api.site,
                            file_format
                        )

                        headers = {
                         'Content-Type': 'multipart/mixed; boundary=boundary-string',
                         'X-Tableau-Auth': ts_api.token,
                         'Host': 'themaggie.local'
                        }
                        
                        if file_format == "twbx":
                            # doesn't work. Call succedes but doesn't create the workbook
                            with open(self.workbook_download_path + '/' + workbook_file_name, 'rb') as f:
                                file_contents = f.read()

                            payload = """--boundary-string\r\nContent-Disposition: name=\"request_payload\"\r\nContent-Type: text/xml\r\n\r\n<tsRequest>\r\n<workbook name=\"{}\" showTabs=\"true\" generateThumbnailsAsUser=\"{}\">\r\n    <project id=\"{}\"/>\r\n    </workbook>\r\n</tsRequest>\r\n--boundary-string\r\nContent-Disposition: name=\"tableau_workbook\"; filename=\"{}\"\r\nContent-Type: application/octet-stream\r\n\r\n{}--boundary-string--\r\n""".format(workbook["name"], ts_api.user, ts_project_id, workbook_file_name, file_contents)

                        else:
                            # works for most but fails from time to time with encoding errors :(
                            # for debug purposes, leaving the main connection here.
                            file_contents = r.content.decode('utf-8')
                            payload = """--boundary-string\r\nContent-Disposition: name=\"request_payload\"\r\nContent-Type: text/xml\r\n\r\n<tsRequest>\r\n<workbook name=\"{}\" showTabs=\"true\" generateThumbnailsAsUser=\"{}\">\r\n    <connections>\r\n                                <connection serverAddress=\"sinch-dw-prod.citavbqulvfl.eu-west-1.redshift.amazonaws.com.\" serverPort=\"5439\">\r\n                                <connectionCredentials name=\"{}\" password=\"{}\"\r\n                                      embed=\"true\" />\r\n                                </connection>\r\n                            </connections>\r\n    <project id=\"{}\"/>\r\n    </workbook>\r\n</tsRequest>\r\n--boundary-string\r\nContent-Disposition: name=\"tableau_workbook\"; filename=\"{}\"\r\nContent-Type: application/octet-stream\r\n\r\n{}--boundary-string--\r\n""".format(workbook["name"], ts_api.user, os.environ.get('TS_CONNECTION_REDSHIFT_USERNAME'), os.environ.get('TS_CONNECTION_REDSHIFT_PASSWORD'), ts_project_id, workbook_file_name, file_contents)

                        response = requests.request('POST', publish_workbook_url, data=payload, headers=headers)
                        print(response.text)

    def existing_project_by_name_on_t_server(self, project_name):
        projects_url = '{}/api/{}/sites/{}/projects?filter=parentProjectId:eq:{},name:eq:{}'.format(
            self.serverUrl,
            self.apiVersion,
            self.site,
            self.parent_project_id,
            project_name
        )

        response = requests.get(
            projects_url,
            headers=self.get_request_headers()
        )

        if len(response.json()["projects"]) > 0 :
            return response.json()["projects"]["project"][0]["id"]
        else:
            return "false"

    def batch_update(self, newuser, newpass):
        """Updates login info to all public and per-project datasources"""
        datasources = []
        public_datasources = self.get_public_datasources()
        for datasource in public_datasources:
            datasources.append(datasource)
        for datasource in self.get_project_datasources():
            datasources.append(datasource)

        logging.info('DATASOURCES IDS')
        logging.info(list(map(lambda x: x['datasource_id'], datasources)))

        for datasource in datasources:
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

        response = requests.put(
            update_url,
            headers=self.get_request_headers(),
            json=payload
        )
        print(response.text)
        return response

    def create_in_tableau_api(self, create_url, payload):
        """Pushes changes update against Tableau's API"""
        response = requests.post(
            create_url,
            headers=self.get_request_headers(),
            json=payload
        )
        print(response.text)
        return response

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
        self.user = response.json()['credentials']['user']['id']

    def logout(self):
        """TODO: some class/method description"""
        # TODO: handle connection errors
        url = '{}/api/{}/auth/signout'.format(self.serverUrl, self.apiVersion)
        requests.post(url, headers=self.get_request_headers())

    def get_public_datasources(self):
        """Get list of public datasource connections, based on datasources"""
        datasources = []
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

                        datasources.append({
                            'datasource_id': source['id'],
                            'datasource_name': source['name'],
                            'datasource_content_url': source['contentUrl'],
                            'connection_id': connection['id'],
                            'connection_type': connection['type'],
                            'connection_server_address':
                                connection['serverAddress'],
                            'connection_server_port': connection['serverPort'],
                            'connection_username': connection['userName']
                        })
        return datasources

    def get_project_datasources(self):
        """Get list of public datasource connections, based on workbooks"""
        datasources = []
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
                        datasources.append({
                            'datasource_id': connection['datasource']['id'],
                            'datasource_name':
                                connection['datasource']['name'],
                            'datasource_content_url': workbook['contentUrl'],
                            'connection_id': connection['id'],
                            'connection_type': connection['type'],
                            'connection_server_address':
                                connection['serverAddress'],
                            'connection_server_port': connection['serverPort'],
                            'connection_username': connection['userName']
                        })
        return datasources

    def get_t_online_projects(self):
        """Get list of projects"""
        entities = []
        projects_url = '{}/api/{}/sites/{}/projects?pageSize=1000'.format(
            self.serverUrl,
            self.apiVersion,
            self.site
        )

        response = requests.get(
            projects_url,
            headers=self.get_request_headers()
        )

        if 'project' in response.json()['projects']:
            projects = response.json()['projects']['project']
            
            for project in projects:
                entities.append({
                    'id': project['id'],
                    'name': project['name'],
                    'description': project['description'],
                    'contentPermissions': project['contentPermissions'],
                    'createdAt': project['createdAt'],
                    'updatedAt': project['updatedAt']
                })

        return entities

    def get_t_server_projects(self):
        """Get list of projects"""
        entities = []
        projects_url = '{}/api/{}/sites/{}/projects?pageSize=1000&filter=parentProjectId:eq:{}'.format(
            self.serverUrl,
            self.apiVersion,
            self.site,
            self.parent_project_id
        )

        response = requests.get(
            projects_url,
            headers=self.get_request_headers()
        )
        
        if 'project' in response.json()['projects']:
            projects = response.json()['projects']['project']
            
            for project in projects:
                entities.append({
                    'id': project['id'],
                    'name': project['name'],
                    'description': project['description'],
                    'contentPermissions': project['contentPermissions'],
                    'createdAt': project['createdAt'],
                    'updatedAt': project['updatedAt']
                })

        return entities
