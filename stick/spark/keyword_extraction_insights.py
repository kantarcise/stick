"""
This script runs two pipelines on content from Reddit comments.

First of all, we setup the raw Dataframe from saved structed data.
Secondly, we use Spark NLP's Yake Keyword Extractor to find keywords from huge concatenated text.
Lastly, we use SentimentDetector to extract info from the high value content. IF the results 
are positive, we save the URL and the result to a csv to be reported to the designers.

"""

# TODO: Setup proper logs.

from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.sql.functions import concat_ws
from pyspark.sql.types import DoubleType

import sparknlp
from sparknlp.annotator import *
from sparknlp.base import *
import json
import os
from datetime import date, datetime
import logging

# Logger
logging.basicConfig(
                    level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[
                        logging.FileHandler(f"{os.getcwd()}/logs/{datetime.now()}-keyword_extraction_insights.log"),
                        logging.StreamHandler(sys.stdout)
                        ]
                    )

stick_logger = logging.getLogger(__name__)

CURRENT_PATH = os.getcwd()
today_specific = date.today().strftime("%d_%m_%y")

class KeywordExtractionInsight():
    """ This class 
    """
    
    def __init__(self,):        
        self.spark = sparknlp.start()
        stick_logger.info("Spark NLP is starting..")
        stick_logger.info(f"Spark NLP version {sparknlp.version()}", )
        stick_logger.info(f"Apache Spark version: {self.spark.version}")
        self.input_json_path = f"{CURRENT_PATH}/data/comments/{today_specific}_reddit.json"
        self.output_csv_path = f"{CURRENT_PATH}/data/comments/{today_specific}_reddit_to_pipeline.csv"
        self.results_csv_path = f"{CURRENT_PATH}/results/{today_specific}_results_reddit.csv"
        
    def __repr__(self) -> str:
        pass

    def __str__(self) -> str:
        pass

    def setup_data(self):
    
        """ Take the latest reddit json file (expected to be run in the same day) and convert it into a something easily used by spark read.
        """
        # TODO: We can turn this code where it uses latest modified file for the input json.

        # make an empty csv
        with open(self.output_csv_path, "w") as f:
            f.write("link,text")
            f.write("\n")
            pass

        # Read the json
        with open(self.input_json_path) as json_file:
            data = json.load(json_file)

        # This data will be cleaned up later so no worries
        for key in data.keys():
            for value in list(data[key].items()):
                temp_key = value[0]
                fulltext = value[1][:]            
                # fulltext is a list of strings.
                # let's save it to a csv row.
                big_string = " ".join(fulltext)
                with open(self.output_csv_path, "a+") as f:
                    f.write(temp_key)
                    f.write(",")
                    f.write(big_string)
                    f.write("\n")

    def keyword_pipeline(self, ):
        
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

        self.df = self.spark.read\
            .option("header", "true")\
            .csv(self.output_csv_path)\
            
        # Clean up the df in case there was a new line in the csv            
        self.df = self.df.filter(F.col("link").contains("old.reddit"))

        self.links_df = self.df.select("link")
        self.contents_df = self.df.select("text")

        self.keywords_result = yake_pipeline.fit(self.contents_df).transform(self.contents_df)
        self.keywords_result = self.keywords_result.withColumn('unique_keywords', F.array_distinct("keywords.result"))

        # Make the array of keywords into a string seperated with comma.
        self.keywords_as_strings = self.keywords_result.select("unique_keywords").withColumn("unique_keywords", concat_ws("," , "unique_keywords")).withColumnRenamed("unique_keywords", "unique_keywords_string")
        print("\n Here are the keywords from the content, in a string seperated with comma \n")
        self.keywords_as_strings.show(truncate= False)

        # TODO: Not used. We need to evaluate the success of this DataFrame too.
        self.keywords_single_string = self.keywords_result.select(F.expr("unique_keywords[0]")).withColumnRenamed("unique_keywords[0]", "unique_keywords_string")
        
        pass

    def sentiment_pipeline(self,):
        
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


        self.sentiment_result = pipeline.fit(self.keywords_as_strings).transform(self.keywords_as_strings)

        # This result is in an array, so before casting we can make it a string,
        self.sentiment_result_cleaned = self.sentiment_result.select("sentimentScore.result") \
                                                .withColumn("result", concat_ws("," , "result"))
        self.sentiment_result_double = self.sentiment_result_cleaned.selectExpr("cast(result as double) result") \
                                .withColumnRenamed("result", "result_score_double")

        # TODO: We can try this later, as this produces less results.
        # second_result = pipeline.fit(keywords_single_string).transform(keywords_single_string)
        # second_result.select("sentimentScore").show(100, truncate= False)
        # sentiment_result.selectExpr("sentimentScore.result").show(100, truncate=False)

        pass

    def report_results(self,):
        
        # Join the links df and the results, with a simple index
        self.links_df_indexed = self.links_df.select("*").withColumn("id1", F.monotonically_increasing_id())
        self.sentiment_result_indexed = self.sentiment_result_double.select("*").withColumn("id2", F.monotonically_increasing_id())
        
        self.joined_result = self.links_df_indexed.join(self.sentiment_result_indexed, F.col("id1") == F.col("id2"), "inner").drop("id1", "id2")
        
        # You can uncomment this to see the whole result.
        # joined_result.select("link", "result_score_double").show(200, truncate = False)
        
        self.joined_cleaned_result = self.joined_result.select("link", "result_score_double").where(F.col("result_score_double") > 0.0)
        
        # Save the results.
        self.joined_cleaned_result.write.mode("overwrite").option("header", "true")\
                                   .csv(self.results_csv_path)
        
        pass

# For Airflow PythonOperator
def nlp_etl_prime():
    robot = KeywordExtractionInsight()
    robot.setup_data()
    robot.keyword_pipeline()
    robot.sentiment_pipeline()
    robot.report_results()

if __name__ == "__main__":
    nlp_etl_prime()