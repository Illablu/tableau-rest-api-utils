from unittest import mock
from TableauApi.TableauApi import TableauApi
import requests

# Tests for batch updating

class MockedResponse():
    def __init__(self, response):
        self.response = response

    def call(self):
        r = requests.Response()
        r.status_code = 200

        def json_func():
            return self.response
        r.json = json_func
        return r

def test_batch_update(mocker):
    mocker.patch.object(TableauApi, 'login')
    mocker.patch.object(TableauApi, 'logout')

    with TableauApi(
            'fakeuser',
            'fakepass',
            serverUrl='server_url',
            apiVersion='666'
            ) as updater:
        mocker.patch.object(
            updater,
            'get_public_datasources',
            return_value=[{
                'datasource_id': '1',
                'datasource_name': 'source name',
                'connection_id': '1'
            }]
        )
        mocker.patch.object(
            updater,
            'get_project_datasources',
            return_value=[{
                'datasource_id': '2',
                'datasource_name': 'source name',
                'connection_id': '2'
            }]
        )
        api_updater_mock = mocker.patch.object(
            updater,
            'update_in_tableau_api')
        updater.site = 'content_url'
        updater.batch_update('new_fakeuser', 'new_fakepass')

        assert api_updater_mock.called is True
        assert api_updater_mock.call_count == 2

        payload = {
          'connection': {
           'userName': 'new_fakeuser',
           'password': 'new_fakepass',
           'embedPassword': True
            }
          }

        assert api_updater_mock.call_args_list == [
          mock.call(
            'server_url/api/666/sites/content_url/datasources/1/connections/1',
            payload
          ),
          mock.call(
            'server_url/api/666/sites/content_url/datasources/2/connections/2',
            payload
          )
        ]

def test_move_projects_to_tableau_server(mocker):
    mocker.patch.object(TableauApi, 'login')
    mocker.patch.object(TableauApi, 'logout')

    # with TableauApi(
    #         'fakeuser',
    #         'fakepass',
    #         serverUrl='server_url',
    #         apiVersion='666'
    #         ) as updater:
    #     mocker.patch.object(
    #         updater,
    #         'get_public_datasources',
    #         return_value=[{
    #             'datasource_id': '1',
    #             'datasource_name': 'source name',
    #             'connection_id': '1'
    #         }]
    #     )
    #     mocker.patch.object(
    #         updater,
    #         'get_project_datasources',
    #         return_value=[{
    #             'datasource_id': '2',
    #             'datasource_name': 'source name',
    #             'connection_id': '2'
    #         }]
    #     )
    #     api_updater_mock = mocker.patch.object(
    #         updater,
    #         'update_in_tableau_api')
    #     updater.site = 'content_url'
    #     updater.batch_update('new_fakeuser', 'new_fakepass')

    #     assert api_updater_mock.called is True
    #     assert api_updater_mock.call_count == 2

    #     payload = {
    #       'connection': {
    #        'userName': 'new_fakeuser',
    #        'password': 'new_fakepass',
    #        'embedPassword': True
    #         }
    #       }

    #     assert api_updater_mock.call_args_list == [
    #       mock.call(
    #         'server_url/api/666/sites/content_url/datasources/1/connections/1',
    #         payload
    #       ),
    #       mock.call(
    #         'server_url/api/666/sites/content_url/datasources/2/connections/2',
    #         payload
    #       )
    #     ]

def test_update_in_tableau_api(mocker):
    updater = TableauApi(
            'fakeuser',
            'fakepass',
            )
    api_updater_mock = mocker.patch.object(requests, 'put')
    expected_headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
    payload = {'key': 'value'}
    updater.update_in_tableau_api('example.com', payload)

    assert api_updater_mock.call_args == mock.call(
        'example.com',
        headers=expected_headers,
        json=payload
    )


def test_get_public_datasources(mocker):
    updater = TableauApi(
            'fakeuser',
            'fakepass',
            serverUrl='server_url',
            apiVersion='666'
            )
    updater.site = 'content_url'

    def datasources_response():
        return MockedResponse({
                'datasources': {
                    'datasource': [{
                        'id': 1,
                        'name': 'source_name',
                        'contentUrl': 'datasource_content_url'
                    }]
                }
            }).call()

    def connections_response():
        return MockedResponse({
                'connections': {
                    'connection': [{
                        'id': 1, 'type': 'connection_type',
                        'serverAddress': 'connection_server_address',
                        'serverPort': 'connection_server_port',
                        'userName': 'connection_username'
                    }]
                }
            }).call()

    mocker.patch.object(requests, 'get', side_effect=[
        datasources_response(), connections_response()
    ])

    assert updater.get_public_datasources() == [{
                    'datasource_id': 1,
                    'datasource_name': 'source_name',
                    'datasource_content_url': 'datasource_content_url',
                    'connection_id': 1,
                    'connection_type': 'connection_type',
                    'connection_server_address': 'connection_server_address',
                    'connection_server_port': 'connection_server_port',
                    'connection_username': 'connection_username'
                }]


def test_get_project_datasources(mocker):
    updater = TableauApi(
            'fakeuser',
            'fakepass',
            serverUrl='server_url',
            apiVersion='666'
            )
    updater.site = 'content_url'

    def workbooks_response():
        return MockedResponse({
                'workbooks': {
                    'workbook': [{
                        'id': 1,
                        'name': 'source_name',
                        'contentUrl': 'datasource_content_url'
                    }]
                }
            }).call()

    def connections_response():
        return MockedResponse({
                'connections': {
                    'connection': [{
                        'id': 1, 'type': 'connection_type',
                        'serverAddress': 'connection_server_address',
                        'serverPort': 'connection_server_port',
                        'userName': 'connection_username',
                        'datasource': {
                            "id": 11,
                            "name": "connection_datasource_name"
                        }
                    }]
                }
            }).call()

    mocker.patch.object(requests, 'get', side_effect=[
        workbooks_response(), connections_response()
    ])

    assert updater.get_project_datasources() == [{
                    'datasource_id': 11,
                    'datasource_name': 'connection_datasource_name',
                    'datasource_content_url': 'datasource_content_url',
                    'connection_id': 1,
                    'connection_type': 'connection_type',
                    'connection_server_address': 'connection_server_address',
                    'connection_server_port': 'connection_server_port',
                    'connection_username': 'connection_username'
                }]
