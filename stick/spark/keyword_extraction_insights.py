"""
This script runs two pipelines on content from Reddit comments.

First of all, we setup the raw Dataframe from saved structed data.
Secondly, we use Spark NLP's Yake Keyword Extractor to find keywords from huge concatenated text.
Lastly, we use SentimentDetector to extract info from the high value content. IF the results 
are positive, we save the URL and the result to a csv to be reported to the designers.

"""

# TODO: Add logger
# TODO: Should we make this a simple class?

from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.sql.functions import concat_ws
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import explode

import sparknlp
from sparknlp.annotator import *
from sparknlp.base import *
import json
import os
from datetime import date

CURRENT_PATH = os.getcwd()
today_specific = date.today().strftime("%d_%m_%y")

def setup_data(input_json_path = f"{CURRENT_PATH}/data/comments/{today_specific}_reddit.json",
               output_csv_path = f"{CURRENT_PATH}/data/comments/{today_specific}_reddit_to_pipeline.csv"):
    
    """ Take the latest reddit json file (expected to be run in the same day) and convert it into a something easily used by spark read.
    """
    # TODO: We can turn this to latest modified for the input file.

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


# First pipeline, Yake

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

# Grateful for this wonderful explanation
# https://github.com/JohnSnowLabs/spark-nlp-workshop/blob/master/tutorials/Certification_Trainings/Public/8.Keyword_Extraction_YAKE.ipynb

df = spark.read\
    .option("header", "true")\
    .csv(f"{CURRENT_PATH}/data/comments/{today_specific}_reddit_to_pipeline.csv")\

# Clean up the df in case there was a new line in the csv            
df = df.filter(F.col("link").contains("old.reddit"))

links_df = df.select("link")
contents_df = df.select("text")

result = yake_pipeline.fit(contents_df).transform(contents_df)
result = result.withColumn('unique_keywords', F.array_distinct("keywords.result"))

# Make the array of keywords into a string seperated with comma.
keywords_as_strings = result.select("unique_keywords").withColumn("unique_keywords", concat_ws("," , "unique_keywords")).withColumnRenamed("unique_keywords", "unique_keywords_string")
print("\n Here are the keywords from the content, in a string seperated with comma \n")
keywords_as_strings.show(truncate= False)

# TODO: Not used. We need to evaluate the success of this DataFrame too.
keywords_single_string = result.select(F.expr("unique_keywords[0]")).withColumnRenamed("unique_keywords[0]", "unique_keywords_string")

# Second Pipeline, Sentiment analysis from those keywords
documentAssembler = DocumentAssembler() \
    .setInputCol("unique_keywords_string") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

lemmatizer = Lemmatizer() \
    .setInputCols(["token"]) \
    .setOutputCol("lemma") \
    .setDictionary(f"{CURRENT_PATH}/content/lemmas_small.txt", "->", "\t")

# If you don't specify EnableScore, the result will be positive or negative, which is not helpful (as 0 is positive on default)
sentimentDetector = SentimentDetector() \
    .setInputCols(["lemma", "document"]) \
    .setOutputCol("sentimentScore") \
    .setEnableScore(True) \
    .setDictionary(f"{CURRENT_PATH}/content/sentiment-dict.txt", ",", ReadAs.TEXT)

pipeline = Pipeline().setStages([
    documentAssembler,
    tokenizer,
    lemmatizer,
    sentimentDetector,
])


sentiment_result = pipeline.fit(keywords_as_strings).transform(keywords_as_strings)

# This result is in an array, so before casting we can make it a string,
sentiment_result_cleaned = sentiment_result.select("sentimentScore.result").withColumn("result", concat_ws("," , "result"))
sentiment_result_double = sentiment_result_cleaned.selectExpr("cast(result as double) result").withColumnRenamed("result", "result_score_double")

# TODO: We can try this later, this produces less results.
# second_result = pipeline.fit(keywords_single_string).transform(keywords_single_string)
# second_result.select("sentimentScore").show(100, truncate= False)
# sentiment_result.selectExpr("sentimentScore.result").show(100, truncate=False)

# Join the links df and the results, with a simple index
links_df_indexed = links_df.select("*").withColumn("id1", F.monotonically_increasing_id())
sentiment_result_indexed = sentiment_result_double.select("*").withColumn("id2", F.monotonically_increasing_id())

joined_result = links_df_indexed.join(sentiment_result_indexed, F.col("id1") == F.col("id2"), "inner").drop("id1", "id2")

# You can uncomment this to see the whole result.
# joined_result.select("link", "result_score_double").show(200, truncate = False)

joined_cleaned_result = joined_result.select("link", "result_score_double").where(F.col("result_score_double") > 0.0)

# Save the results.
joined_cleaned_result.write.mode("overwrite").option("header", "true")\
                           .csv(f"{CURRENT_PATH}/results/{today_specific}_results_reddit.csv")

