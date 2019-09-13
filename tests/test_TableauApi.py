from unittest import mock
from TableauApi.TableauApi import TableauApi

# Tests for batch updating


def test_batch_update(mocker):
    def assign_public_datasources():
        expected_datasource = {
          'datasource_id': '1',
          'datasource_name': 'source name',
          'connection_id': '1'
        }
        updater.datasources.append(expected_datasource)

    def assign_project_datasources():
        expected_datasource = {
         'datasource_id': '2',
         'datasource_name': 'source name',
         'connection_id': '2'
        }
        updater.datasources.append(expected_datasource)

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
            side_effect=assign_public_datasources
        )
        mocker.patch.object(
            updater,
            'get_project_datasources',
            side_effect=assign_project_datasources
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
