from stick.crawler.reddit_crawler import RedditCrawler
import pytest

def test_reddit_crawler_validity():
    crawler = RedditCrawler()
    assert crawler.subreddits != None

def test_reddit_crawler_adblock():
    crawler = RedditCrawler()
    assert crawler.adblock_folder_name != None