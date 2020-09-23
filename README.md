# dwdcdc

Fool around with data from DWD Climate Data Center.

## Purpose

Download and update data from the Deutscher Wetterdienst (DWD) Climate Data Center (CDC) into a local database to support future analysis, e.g. from automatic scripts (this repository) or Jupyter notebooks (other repository). 

## Features

- support the following data sources
  - air temperature 2m, hourly values
- functions
  - download data from DWD CDC
  - avoid redundant downloads
  - limit download to stations of interest via `.ini` file
  - store in database

## Development Pattern

This project is built for Jenkins (or cron) based usage. 
This means that the content of this repository will 
periodically be executed from a Jenkins instance or via cron
according to the following pattern:
- clone repository from GitHub into local workspace or temp directory
- execute sevral scripts programatically
   - leaving logs behind
   - sending mail about success or failure (utilizing my `johanna` module)
- exit (and optionally remove workspace or temp directory)

We thus develop for an ever refreshed git workspace in an execution environment. 

**Side note:** Because of some unfortunate "feature" of my favourite IDE PyCharm, we go for a package that will be installed in development mode on the production machine also and can be updated by pulling the GitHub repo before executing the background job. I had to get rid of [the dash in the original repo name](https://github.com/turkishmaid/dwd-cdc) because of this.


 
