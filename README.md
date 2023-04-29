# stick

**Stick** is a simple & complete batch processing pipeline. It fetches data from the
best subreddits in the world and reports the ones which have the most (positive) attention.

This report can be used to make content to help creators about current/upcoming trends, which [Remini](https://apps.apple.com/us/app/remini-ai-photo-enhancer/id1470373330) and [Splice](https://apps.apple.com/mg/app/splice-video-editor-maker/id409838725) serve.


<p align="center">
  <img src="content/stick.png" title="Fundemental of a Fire" width="30%" height="30%"/>
</p>

## Design

There will be a design image here.

## Installation For Ubuntu

This project uses [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html) to work in a clear environment.

After your installation of conda, you can use requirements.txt for the environment you made.

```sh
$ conda create --name <MY_ENVIRONMENT> python==3.8.12
$ pip install -r requirements.txt
```

For a fresh installation, we will need a specific version of Chrome and AdBlocker Extension to make Stick work. Luckily, they are pretty straightforward commands.

```
wget --no-verbose -O /tmp/chrome.deb https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_103.0.5060.53-1_amd64.deb \
  && apt install -y /tmp/chrome.deb \
  && rm /tmp/chrome.deb
```

After this step, simply open Chrome and install [AdBlocker](https://chrome.google.com/webstore/detail/adblock-%E2%80%94-best-ad-blocker/gighmmpiobklfepjocnamgkkbiglidom) from extensions.

As we use Apache Spark and Spark NLP, we are going to need JAVA to be installed aswell. You can look it up online or simply:

```
$ sudo apt-get install openjdk-8-jdk
# Confirm your installation
$ java --version
```

After java installation, you can setup the [java path on bashrc](https://stackoverflow.com/a/9612986).

```
$ sudo nano ~/.bashrc

# To end of the file, add the following:

# >>> Java Setup >>>
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
# <<< Java Setup <<<

# Save and close the bashrc file

# Apply changes
$ source ~/.bashrc

```

## Usage

Although Stick is meant to be orchestrated and run automatically, here are the commands you can use to run it manually, in order.

```sh
# Activate your freshly made environment
$ conda activate <MY_ENVIRONMENT>

# Run the crawler
$ python stick/crawler/reddit_crawler.py

# Run the pipeline
$ python stick/spark/keyword_extraction_insights.py 
```

## Usage With Airflow

If you want to use Airflow and have the fully autonomous experience, you can install it with pip! Here are the [documents.](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#installation-and-upgrade-of-airflow-core) While using Airflow, we only need the core parts.

```
$ conda activate <MY_ENVIRONMENT>
$ AIRFLOW_VERSION=2.5.3
$ PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.7
$ CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-no-providers-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.5.3/constraints-no-providers-3.7.txt

$ pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```
These commands will install Airflow for us in our environment.

To Open up Airflow, open three different terminals and run these commands in order:

```
$ airflow db init
$ airflow webserver
$ airflow scheduler
```

The Airflow UI link will appear at the webserver terminal. By default, Airflow requires users to specify a password prior to login. 

```
$ airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
```

Now with your new user, go to "0.0.0.0/8000" on your browser and login. 

When you logged in, you'll see two warnings, which is why we are going to do the following. 

In default, Airflow uses a SQLLite DB to store all the metadata about it's processes. This structure only allows a SequentialExecutor for the Airflow itself.

We want to use Airflow in [LocalExecutor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/local.html) instead of Sequential, so we need to [upgrade the DB](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html) from the installed SQLLite database on default.

Install PostgreSQL, with the help of [this resource](https://www.digitalocean.com/community/tutorials/how-to-install-postgresql-on-ubuntu-20-04-quickstart)

```
$ sudo apt update
$ sudo apt install postgresql postgresql-contrib
# Ensure the service started
$ sudo systemctl start postgresql.service
```

Now that we have PostgreSQL, we need to link it to airflow. [This](https://www.youtube.com/watch?v=4UHhqa-4_Sw) is a great resource on the matter.

After you have your sqlalchemy string, you can change the configuration in /home/<YOUR_USER>/airflow/airflow.cfg

There are 4 things you want to change here.

1) **dags_folder** This will be the path of your dags folder in your repository.
2) **executor** After the installation of PostgreSQL, we can change this to 'LocalExecutor'
3) **load_examples** If you don't make this "False" there will be a lot of things on UI that might confuse you.
4) **sql_alchemy_conn** This is the way we bridge PostgreSQL and Airflow. Depending on your configuration it will look something like "sql_alchemy_conn = postgresql+psycopg2://USER:PASSWORD@IP:PORT/DATABASE_NAME
"

## Current State

Stick is currently running on a GCP server with a Ubuntu 20.04 VM instance. This instance is a e2-standart-2, it has 2vCPU's and 8 GB's of memory. Daily cost is around $0.8.

## Work In Progress:

- [ ] The readme itself.
- [x] Documentation for MVP.
- [x] First version that accomplishes full data to value cycle.
- [ ] So far, we are just keeping logging in the machine. That won't cut it. Need GCP logs.
- [ ] No error reporting - If possible, need to use sentry. As the data sinks become various, this will be an issue.
- [x] Orchestration - Airflow can be used. Done on /development-airflow branch.
- [ ] Airflow with proper setup. Local Executor & PostgreSQL.
- [x] Sentiment dictionaries for SparkNLP.
- [ ] Monitoring is done manually. Grafana can be setup on gcp.
- [ ] Almost no scale at all. What if there was a huge data flow coming in from multiple sinks, which cannot be saved/stated in a single file? (We can use a Kafka container for this.) 
- [ ] Model testing and quality assurrance ? - Need to setup a test folder that exactly does that.
