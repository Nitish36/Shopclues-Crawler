import time
import os
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import re
import gspread
from gspread_dataframe import set_with_dataframe
import schedule
import mysql.connector

def url_generator(driver):
    item_list = []
    ctr = 0

    while ctr <= 100:
        items_links = driver.find_elements(By.CLASS_NAME, 'column.col3.search_blocks a')

        for item_link in items_links:
            item_list.append(item_link.get_attribute('href'))
            ctr += 1
            if ctr == 100:  # Break when 50 items are collected
                break

        try:
            button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.ID, 'moreProduct'))
            )
            # Scroll the button into view
            actions = ActionChains(driver)
            actions.move_to_element(button).perform()

            # Wait for a short while to ensure stability
            time.sleep(50)

            # Click the button
            button.click()
        except Exception as e:
            print(f"Error: {str(e)}")
            break  # Break if there's an error or button not found
    print(ctr)
    return item_list


def generate_data():

    url_list = [
        'https://www.shopclues.com/search?q=Mobiles&z=0&user_id=&user_segment=default&trend=1',
        'https://www.shopclues.com/search?q=Laptop&sc_z=&z=0&count=10&user_id=&user_segment=default',
        'https://www.shopclues.com/search?q=bulbs%20&sc_z=&z=0&count=9&user_id=&user_segment=default',
        'https://www.shopclues.com/search?q=battery&sc_z=&z=0&count=10&user_id=&user_segment=default',
        'https://www.shopclues.com/search?q=lamps&sc_z=&z=0&count=5&user_id=&user_segment=default',
        'https://www.shopclues.com/search?q=steel%20bottles&sc_z=&z=0&count=10&user_id=&user_segment=default',
        'https://www.shopclues.com/search?q=boat&sc_z=&z=0&count=9&user_id=&user_segment=default',
        'https://www.shopclues.com/search?q=Television&auto_suggest=1&seq=1&type=keyword&token=televisi&count=10&user_id=&user_segment=default&z=0',
        'https://www.shopclues.com/search?q=Shoes&z=0&user_id=&user_segment=default&trend=1'
    ]

    options = webdriver.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    service = Service(executable_path="C:\\Program Files (x86)\\chromedriver-win64\\chromedriver.exe",options = options)
    driver = webdriver.Chrome(service=service)
    driver2 = webdriver.Chrome(service=service)
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.53 Safari/537.36'})
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    #url_list = url_generator(driver)
    #print(url_list)
    scrapped_data = []
    item_list = []
    for link in url_list:
        driver2.get(link)
        item_list = url_generator(driver2)
        for url in item_list:
            driver.get(url)
            WebDriverWait(driver,100).until(
                EC.presence_of_element_located((By.CLASS_NAME,'prd_mid_info '))
            )
            pagesource = driver.page_source
            soup = BeautifulSoup(pagesource,'lxml')
            item_name = soup.find('h1',attrs={'itemprop':'name'}).text.strip()
            description = soup.find('span',attrs={'itemprop':'description'}).text.strip()
            pid = soup.find('span',attrs={'class':'pID'}).text.strip().replace('Product Id :','').strip()
            productid = re.search(r'\d+', pid).group()
            cat = soup.find('span',attrs={'class':'pID'}).text.strip().replace('Product Id :','').strip()
            category = [re.search(r'\D+', cat).group().strip() if re.search(r'\D+', cat) else ''][0]
            price = soup.find('span',attrs={'class':'f_price'}).text.strip().replace('₹','')
            deal_price = soup.find('span',attrs={'class':'low_price'}).text.strip().replace('₹','').replace(',','')
            old_price = [soup.find('span',attrs={'class':'o_price1'}).text.strip().replace('₹','').replace(',','').replace('MRP:','') if soup.find('span',attrs={'class':'o_price1'}) else 0][0]
            discount_percent = [soup.find('span',attrs={'class':'discount'}).text.replace('%','').replace('off','').strip() if soup.find('span',attrs={'class':'discount'}) else 0][0]
            star = driver.find_element(By.XPATH,'//*[@id="pdp_rnr"]/div[1]/div/div[2]/p').text.strip().split()[0]
            review_count = driver.find_element(By.XPATH,'//*[@id="pdp_rnr"]/div[1]/div/div[2]/p').text.strip().split()[2]
            path_extractor = soup.find('div',attrs={'class':'sllr_info'})
            place = path_extractor.find('p').text.strip().split(',')[0]
            extracted_text = path_extractor.find('p').text.strip()
            split_text = extracted_text.split(',')
            type_item = ''
            if len(split_text) > 1:
                state = split_text[1].strip()
            else:
                state = ""
            split_url = link.split('?')
            params = split_url[1]
            param_list = params.split('&')
            for param in param_list:
                if param.startswith('q='):
                    type_item = param.split('=')[1]
            data = {
                'product_id':productid,
                'type':type_item,
                'category':category,
                'name':item_name,
                'description':description,
                'price':int(price),
                'deal_price':int(deal_price),
                'old_price':int(old_price),
                'discount%':int(discount_percent),
                'star':star,
                'review_count':review_count,
                'place':place,
                'state':state,
            }
            scrapped_data.append(data)
    item_data = pd.DataFrame(scrapped_data)
    return item_data

def write_df(**kwargs):
    GSHEET_NAME = 'ShopcluesFeeder'
    TAB_NAME = 'Content'
    credentialsPath = os.path.expanduser("credentials\\diamond-analysis-ac6758ca1ace.json")
    df = generate_data()
    if os.path.isfile(credentialsPath):
        # Authenticate and open the Google Sheet
        gc = gspread.service_account(filename=credentialsPath)
        sh = gc.open(GSHEET_NAME)
        worksheet = sh.worksheet(TAB_NAME)

        set_with_dataframe(worksheet, df)


        print("Data loaded successfully!! Have fun!!")
    else:
        print(f"Credentials file not found at {credentialsPath}")

def feed_database():
    item_data = generate_data()
    item_data_tuples = [tuple(row) for row in item_data.to_numpy()]
    mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="$Freeman_007$",
            auth_plugin = 'mysql_native_password'
        )
    mycursor = mydb.cursor(buffered = True)
    mycursor.execute("CREATE DATABASE IF NOT EXISTS shopclues")
    mycursor.execute("SHOW DATABASES")
    mycursor.execute("USE shopclues")
    mycursor.execute("""CREATE TABLE IF NOT EXISTS shop (
                product_id VARCHAR(20),
                type VARCHAR(20),
                category VARCHAR(20),
                name VARCHAR(255),
                description VARCHAR(300),
                price INT,
                deal_price INT,
                old_price INT,
                discount FLOAT,  
                star INT,
                review_count INT,
                place VARCHAR(20),
                state VARCHAR(20)
                )
            """
        )
    mycursor.execute("SHOW TABLES")
    ins = """
            INSERT INTO shop (
                product_id, type, category, name, description, price, deal_price,
                old_price, discount, star, review_count, place, state
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

    # Execute the INSERT statement for each item in item_list
    mycursor.executemany(ins, item_data_tuples)
    mydb.commit()
    mycursor.execute("SELECT * FROM shop")
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)
    print("\n")
    if len(myresult)>=1511:
        mycursor.execute("DELETE FROM shop")
        mydb.commit()
        print("Data deleted to prevent overloading of SQL database")

feed_database()
schedule.every(15).seconds.do(write_df)
while True:
    schedule.run_pending()
    time.sleep(3)