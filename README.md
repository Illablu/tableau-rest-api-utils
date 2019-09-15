# Utilities for Tableau REST API
## Table of contents
TODO.

## Use case
Running repetitive tasks in Tableau Server/Online could be tricky if you have several dozens of workbooks/datasource.

The following library provides a series of customizable utils to simplify a few common tasks, such as:
- Batch update of Datasource credentials
- Batch download and upload of Datasources and Workbooks (planned)
- etc.

## Notes
### Batch Update Credentials
Script will look up public Datasources and Workbook's specific Datasources and iterate through them to update the connection credentials.

Note that this might take a while depending on the number of Datasources you have.

You will need credentials of a User with Admin access.

The script will attempt to login, connect to list your Datasources, update them, and logout. Please be aware of any API quota you might incur in (if any).

## Requirements
Software:
- Python 3.6.~
- pipenv

Tableau:
- Admin User Datasource Credentials

## Installation

```
pipenv install
```

## Applies to
- Tableau Server
- Tableau Online

## Usage
Either fire up a Python console and run the script:
```python
import requests
import os
from TableauApi import TableauApi

with TableauApi() as api:
  api.batch_update("guybrush","threepwood")
```
or run the sample script provided after you renamed to a python script and customized to your use:
```
python test.py
```

## Development
Tests can run in a virtual environment using:
```
pipenv run ptw
```

## Development plan
- [x] Tests environment
- [x] Debug lib
- [x] Tests and support for batch (all datasources) credential update functionality
- [ ] Tests and support for single-datasource credential update functionality
- [ ] Error handling 
- [ ] Library wrapping / Put API login/logout in their own classes
- [ ] Generate documentation, copyright info, disclaimer and table of contents
- [ ] Support for dry-run

## Disclaimer
TODO.

## Copyright
Copyright 2019 Sinch ABL