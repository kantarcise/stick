"""
This script crawls Reddit and fetches popular comments on posts.
"""

# TODO: Config file relative import ?
# TODO: DATABASE - Not Connected, do we really need it?
# TODO: Updated subreddits. --- https://www.reddit.com/best/communities/1/

import sys
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

import time
import pandas as pd
import os
from datetime import date, datetime
# Not used
#import psycopg2
import json

project_root = "/home/sezai/repositories/stick"

# Logger
logging.basicConfig(
                    level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[
                        logging.FileHandler(f"{project_root}/logs/{datetime.now()}-reddit_crawler.log"),
                        logging.StreamHandler(sys.stdout)
                        ]
                    )

stick_logger = logging.getLogger(__name__)

CURRENT_PATH = project_root
CHROME_DRIVER_PATH = f"{CURRENT_PATH}/utils/chromedriver"
ADBLOCK = '/home/sezai/.config/google-chrome/Default/Extensions/gighmmpiobklfepjocnamgkkbiglidom/'

# TODO: READ FROM CONFIG FILE 
# Configuration = ConfigFile()

# CONN_STRING = "postgres://" + Configuration.get_username() + ":" + Configuration.get_password() + "@" + Configuration.get_host() + "/" + Configuration.get_port()
CONN_STRING = "postgresql://" + "admin" + ":" + "admin" + "@" + "localhost"+ ":" + "5432"+ "/" + "reddit"

class RedditCrawler():
    """ This class is used for fetching data from Reddit, the front page of the Internet!
    """
    def __init__(self):
        try:
            # Ad blocker and Selenium Options
            self.adblock_folder_name = os.listdir(ADBLOCK)[0]
            self.unpacked_extension_path = os.path.join(ADBLOCK + self.adblock_folder_name)
            self.options = Options()
            self.options.add_argument("--window-size=1920,1200")
            self.options.add_argument("--disable-infobars")
            self.options.add_argument("--load-extension={}".format(self.unpacked_extension_path))

            # Data path to be saved!
            self.today_specific = date.today().strftime("%d_%m_%y")
            # Data path to be saved!
            self.data_path = CURRENT_PATH + "/data/raw/" + self.today_specific + "_reddit_data.csv"
            self.content_dict = {}
            # Subreddits to check
            self.subreddits = ["memes", "gaming", "lol", "pics", "food", "funny", "Damnthatsinteresting", 
                                "interestingasfuck", "technology", "clevercomebacks", "videos", "space", 
                                "aww","Art" , "gifs", "InternetIsBeautiful", "travel"]        
            stick_logger.info("All variables setup, connecting to database...")
        except:
            stick_logger.exception("Could not initialize Reddit Crawler. Please check the project structure.")

        # Database Connection
        try:
            # For now no dbs.
            # self.pg_conn = psycopg2.connect(database = "reddit", user = "admin", password = "admin", host = "localhost" , port = "5432" )
            # self.cursor = self.pg_conn.cursor()
            # self.cursor.execute('DROP DATABASE IF EXISTS python_db')
            # self.cursor.execute('CREATE DATABASE python_db')
            # stick_logger.info("Connected to the database")
            pass
        except: 
            stick_logger.exception("Could not connect to the database.")
        
    def setup(self):
        """ This method opens up a new instance of Chrome and fetches all the popular content for chosen subreddit into a csv.
        """
        # For Reddit!
        self.lenght_list = []
        self.content_list = []

        driver = webdriver.Chrome(options = self.options, executable_path= CHROME_DRIVER_PATH)
        time.sleep(2)
        driver.close()
        driver.switch_to.window(driver.window_handles[-1])
        driver.minimize_window()

        for x in range(len(self.subreddits)):
            stick_logger.info(f"Starting to look data from {self.subreddits[x]}")

            # First subreddit
            driver.get("https://old.reddit.com/r/" + self.subreddits[x])
            driver.implicitly_wait(1)

            # Click for best posts
            top_button = driver.find_element(By.XPATH, "//*[@id='header-bottom-left']/ul/li[5]/a")
            top_button.click()
            driver.implicitly_wait(2)

            # Get the elements
            key_element = driver.find_elements(By.CLASS_NAME, "bylink")
            #print("Lenght of posts list: ", len(key_element))
            self.lenght_list.append(len(key_element))

            content_list = []
            number_of_comments = []
            for i in key_element:
                content_list.append(i.get_attribute('href'))
                number_of_comments.append(i.text) 
            
            # Put all the content into a list
            self.content_list.append(content_list) 
            # You can uncomment this if you want to see the content logged in terminal.
            stick_logger.info(self.content_list)               
            
            # For the selected subreddit, add all the content with its key to the dictionary.
            self.content_dict.update( {self.subreddits[x]: content_list})
            driver.implicitly_wait(1)

            stick_logger.info(f"Ended process for, r/{self.subreddits[x]} subreddit!")
            
        driver.implicitly_wait(3)
        # Make a dataframe from the dictionary.
        data = pd.DataFrame.from_dict(self.content_dict, orient='index')
        # Save the dictionary as a CSV
        data.to_csv(self.data_path)
        stick_logger.info(f"Saved all the data from {self.subreddits} into {self.data_path}")


    def get_best_comments(self, number_of_comments_to_be_saved = 8):
        
        __subreddits_dict = {}
        __comment_counter = 0

        # Read the URL's from csv.
        __urls_dataframe = pd.read_csv(self.data_path)
        __urls_dataframe = __urls_dataframe.transpose()
        # Change the headers with the second row (https://stackoverflow.com/questions/26147180/convert-row-to-column-header-for-pandas-dataframe)
        __headers = __urls_dataframe.iloc[0]
        __urls_dataframe  = pd.DataFrame(__urls_dataframe.values[1:], columns = __headers)

        __base_xpath = "/html/body/div[4]/div[2]/div[3]/div[1]/div[2]/form/div/div/p"
        # Possibly
        # /html/body/div[4]/div[2]/div[3]/div[5]/div[2]/form/div/div/p/a  OR  /html/body/div[4]/div[2]/div[3]/div[7]/div[2]/form/div/div/p[1]
        
        # Initialize Chrome
        driver = webdriver.Chrome(options = self.options, executable_path= CHROME_DRIVER_PATH)
        # Give time to initialize Ad Blocker popup to load.
        time.sleep(2)
        driver.close()
        driver.switch_to.window(driver.window_handles[-1])
        driver.minimize_window()


        # Need a loop for every subreddit.
        for index, subreddit in enumerate(self.subreddits):
            stick_logger.info(f"Starting to look at the data from r/{subreddit}")

            # Get the link list
            __link_list  = [ x for x in __urls_dataframe[subreddit].to_list() if pd.isnull(x) == False ]
            __subreddits_dict[subreddit] = {}

            # Another loop for every link.
            for link in __link_list:
                        # sort by top
                if "/user/" in link:
                    continue
                driver.get(link + "?sort=top")
                driver.implicitly_wait(1)

                __subreddits_dict[subreddit][link+"?sort=top"] = []
                # Check for best top 8 comments within that link, one by one.
                for i in range(1,((number_of_comments_to_be_saved * 2) + 1 ),2):
                    try:
                        configured_base_xpath = __base_xpath[:36] + str(i) + __base_xpath[37:]
                        comment = driver.find_element(By.XPATH, configured_base_xpath).text
                        __subreddits_dict[subreddit][link+"?sort=top"].append(comment)
                        __comment_counter = __comment_counter + 1
                    except Exception as e:
                        print(f"The comment that you are looking for is not here!")
                        # No need to look for the comments in that link anymore.
                        break

                stick_logger.info(f"Ended checking comments for, r/{link}")
                
            stick_logger.info(f"Ended process for the subreddit: {subreddit}")

        driver.implicitly_wait(3)
        driver.close()

        with open(f"{CURRENT_PATH}/data/comments/{self.today_specific}_reddit.json", "w+") as outfile:
            json.dump(__subreddits_dict,outfile)
        
        stick_logger.info(f"Total comments looked up is : {__comment_counter}")
        stick_logger.info(f"Saved all the valuable content to :{CURRENT_PATH}/data/comments/{self.today_specific}_reddit.json")

# This is mainly for Airflow's PythonOperator.
def prime():
    stick_logger.info("Reddit Crawler is Starting!")
    Crawler = RedditCrawler()
    stick_logger.info("Seaching the frontpage of the internet!")
    Crawler.setup()
    stick_logger.info("Getting the best comments!")
    Crawler.get_best_comments(number_of_comments_to_be_saved = 8)
    stick_logger.info("Successfully fetched data!")
    stick_logger.info("Bye!")

def dummy():
    pass

if __name__ == "__main__":
    prime()