from stick.spark.keyword_extraction_insights import KeywordExtractionInsight
import pytest

def test_keyword_extraction_input_file():
    extractor = KeywordExtractionInsight()
    assert extractor.input_json_path != None