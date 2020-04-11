# Github Commits ETL

## Content
* [Summary](#Summary)
* [Project structure](#Project-structure)
* [Installation](#Installation)

This README assumes the user has Docker and pip3 installed locally.

### Summary
This project is an ETL pipeline which curls the [Github Commits API](https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository) endpoint. The ETL pipeline allows 4 parameters to be inserted (GitHub owner/organization name, GitHub repository name, start date, end date). The data extracted from the endpoint is then transformed and inserted into a Postgres DB, along with a few insights created.

The image <u>output/heatmap_example.png</u> was generated using ```python3 main.py --organization='apache' --repository='airflow' --start_date='2020-03-20T00:00:00' --end_date='2020-04-10T00:00:00'```.

### Project Structure
1. main.py
    * Contains CLI for triggering ETL pipeline.
2. helpers.py
    * Contains helper functions for data manipulation.
3. sql_queries.py
    * Contains SQL queries for creation, insertion and deletion of the tables.
4. configs.cfg
    * Base config setup to spin up project locally.

### Installation
To prevent messing up with your local Python dependencies, one is highly encouraged to setup a virtual environment to run this code.
1. Clone this repository
    - ```git clone https://github.com/terryyylim/github-commits-etl.git```

2. Setup virtual environment.
    - Install virtualenv
        - ```pip3 install virtualenv```
    - Create virtualenv
        - ```virtualenv -p python3 <desired-path>```
    - Activate virtualenv
        - ```source <desired-path>/bin/activate```
    - Install dependencies
        - ```pip3 install -r requirements.txt```

3. Run postgres docker image on the background
    - Build docker image
        - ```docker build -t ghcommits .```
    - Run docker image on background
        - ```docker run -d -p 5432:5432 --name gh_commits ghcommits```

4. Run ETL script
    - Running with default parameters
        - ```python3 main.py```
    - Running with input parameters
        - ```python3 main.py --organization='apache' --repository='hadoop' --start_date='2020-03-20T00:00:00' --end_date='2020-04-10T00:00:00'```

5. Query Database
    - Execute interactive bash on Postgres docker container.
        - ```docker exec -it gh_commits /bin/bash```
    - Execute terminal-based PostgreSQL interface.
        - ```psql -U postgres```
        - ```\c ghcommits_postgres```
    - Run PostgreSQL queries.