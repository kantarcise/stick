# stick

**Stick** is a simple & complete batch processing pipeline. It fetches data from the
best subreddits in the world and reports the ones which have the most (positive) attention.

This report can be used to make content to help creators about current/upcoming trends, which [Remini](https://apps.apple.com/us/app/remini-ai-photo-enhancer/id1470373330) and [Splice](https://apps.apple.com/mg/app/splice-video-editor-maker/id409838725) serve.


<p align="center">
  <img src="content/stick.png" title="Fundemental of a Fire" width="30%" height="30%"/>
</p>

## Design

test

## Install

This project uses [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html) to work in a clear environment.

After your installation of conda, you can use requirements.txt for the environment you made.

```sh
$ conda create --name <MY_ENVIRONMENT> python==3.8.12
$ pip install -r requirements.txt
```

## Usage

Although it's meant to be orchestrated and run automatically, here are set of commands you can use to run stick manually on Ubuntu PC.

```sh
# Activate your freshly made environment
$ conda activate <MY_ENVIRONMENT>

# Run the crawler
$ python stick/crawler/reddit_crawler.py

# Run the pipeline
$ python stick/spark/keyword_extraction_insights.py 
```


## Work In Progress:

- [ ] The readme itself.
- [ ] So far, we are just logging into the local machine. That won't work. Need GCP logs.
- [ ] No error reporting - If possible, need to use sentry.
- [ ] No monitoring - Grafana can be setup on gcp.
- [ ] No scaling at all. What if there is a huge dataflow coming in that cannot be saved in a single file? (We can use a Kafka container for this.) 
- [ ] Orchestration - Airflow can be used.
- [ ] Model testing and quality assurrance ? - Need to setup a test folder that exactly does that.
