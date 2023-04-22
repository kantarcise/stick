"""
This script extracts keywords from Reddit comments using Yake.
"""

from pyspark.sql import functions as F
from pyspark.ml import Pipeline

import sparknlp
import pandas as pd

from pyspark.ml import PipelineModel
from sparknlp.annotator import *
from sparknlp.base import *
import json
import os
from datetime import date, datetime


CURRENT_PATH = os.getcwd()
today_specific = date.today().strftime("%d_%m_%y")

def setup_data(input_json_path = f"{CURRENT_PATH}/data/comments/{today_specific}_reddit.json",
               output_csv_path = f"{CURRENT_PATH}/data/comments/{today_specific}_reddit_to_pipeline.csv"):
    
    """ Take the latest reddit json file and convert it into a something easily used by spark read.
    """
    # make the empty csv
    with open(output_csv_path, "w") as f:
        f.write("link,text")
        f.write("\n")
        pass

    # Read the json
    with open(input_json_path) as json_file:
        data = json.load(json_file)

    # This will be cleaned up later so no worries
    for key in data.keys():
        for value in list(data[key].items()):
            temp_key = value[0]
            fulltext = value[1][:]            
            # fulltext is a list of strings.
            # let's save it to a csv row.
            big_string = " ".join(fulltext)
            with open(output_csv_path, "a+") as f:
                f.write(temp_key)
                f.write(",")
                f.write(big_string)
                f.write("\n")


setup_data()


spark = sparknlp.start()

print("Spark NLP is starting..")
print("Spark NLP version", sparknlp.version())
print("Apache Spark version:", spark.version)

stopwords = StopWordsCleaner().getStopWords()

document = DocumentAssembler() \
            .setInputCol("text") \
            .setOutputCol("document")

sentenceDetector = SentenceDetector() \
            .setInputCols("document") \
            .setOutputCol("sentence")

token = Tokenizer() \
            .setInputCols("sentence") \
            .setOutputCol("token") \
            .setContextChars(["(", ")", "?", "!", ".", ","])

keywords = YakeKeywordExtraction() \
            .setInputCols("token") \
            .setOutputCol("keywords") \
            .setMinNGrams(1) \
            .setMaxNGrams(3)\
            .setNKeywords(7)\
            .setStopWords(stopwords)

yake_pipeline = Pipeline(stages=[document, sentenceDetector, token, keywords])

# If you want to take a look for the text example here is the link
# https://github.com/JohnSnowLabs/spark-nlp-workshop/blob/master/tutorials/Certification_Trainings/Public/8.Keyword_Extraction_YAKE.ipynb

df = spark.read\
                .option("header", "true")\
                .csv(f"{CURRENT_PATH}/data/comments/{today_specific}_reddit_to_pipeline.csv")\
                
df = df.filter(F.col("link").contains("old.reddit"))

links_df = df.select("link")
contents_df = df.select("text")

result = yake_pipeline.fit(contents_df).transform(contents_df)

result = result.withColumn('unique_keywords', F.array_distinct("keywords.result"))

# result.show()

keywords = result.select("unique_keywords").show(truncate = False)
result.printSchema()
result.select("unique_keywords").write.format("json").save(f"{CURRENT_PATH}/results.json")

only_unique_keywords_df = result.select("unique_keywords")

# keywords.write.format("json").save(CURRENT_PATH + "results.json")

# TODO: Fix the DATA
# TODO: Join the results on a df and than a csv

# Sentiment analysis from those keywords
documentAssembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

lemmatizer = Lemmatizer() \
    .setInputCols(["token"]) \
    .setOutputCol("lemma") \
    .setDictionary(f"{CURRENT_PATH}/content/lemmas_small.txt", "->", "\t")

sentimentDetector = SentimentDetector() \
    .setInputCols(["lemma", "document"]) \
    .setOutputCol("sentimentScore") \
    .setDictionary(f"{CURRENT_PATH}/content/sentiment-dict.txt", ",", ReadAs.TEXT)

pipeline = Pipeline().setStages([
    documentAssembler,
    tokenizer,
    lemmatizer,
    sentimentDetector,
])

data = spark.createDataFrame([
    ["The staff of the restaurant is nice"],
    ["I recommend others to avoid because it is too expensive"]
]).toDF("text")


result = pipeline.fit(data).transform(data)

result.selectExpr("sentimentScore.result").show(truncate=False)


