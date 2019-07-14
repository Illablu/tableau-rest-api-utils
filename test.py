import requests
import os
from TableauApi import TableauApi

# begin ; set -lx TS_USERNAME;
with TableauApi() as api:
	api.batch_update()