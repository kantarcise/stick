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

def setup_data(input_json_path = "/home/sezai/sheaf/data/comments/30_01_23_reddit.json",
               output_csv_path = "/home/sezai/sheaf/data/comments/reddit_to_pipeline_30.csv"):
    
    """ Take the latest reddit json file and convert it into a something easily used by spark read.
    """
    # make the empty csv
    with open(output_csv_path, "w") as f:
        f.write("text")
        f.write("\n")
        pass

    # Read the json
    with open(input_json_path) as json_file:
        data = json.load(json_file)

    # print(list(data["memes"].values())[0])

    for key in data.keys():
        for value in list(data[key].values()):
            fulltext = value[:]
            # fulltext is a list of strings.
            # let's save it to a csv row.
            big_string = " ".join(fulltext)
            with open(output_csv_path, "a+") as f:
                f.write(big_string)
                f.write("\n")

    

setup_data()

spark = sparknlp.start() # for GPU training >> sparknlp.start(gpu = True) # for Spark 2.3 =>> sparknlp.start(spark23 = True)

print("Spark NLP is starting..")
print("Spark NLP version", sparknlp.version())
print("Apache Spark version:", spark.version)

stopwords = StopWordsCleaner().getStopWords()
# print(f"Here are some stopwords for you {stopwords[:5]}")    

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

# To look for the text example here is the link
# https://github.com/JohnSnowLabs/spark-nlp-workshop/blob/master/tutorials/Certification_Trainings/Public/8.Keyword_Extraction_YAKE.ipynb

df = spark.read\
                .option("header", "true")\
                .csv("/home/sezai/sheaf/data/comments/reddit_to_pipeline_30.csv")\
                
df.show(truncate=100)

result = yake_pipeline.fit(df).transform(df)
result = result.withColumn('unique_keywords', F.array_distinct("keywords.result"))

result.show()

result.select("unique_keywords").show(truncate= False)
