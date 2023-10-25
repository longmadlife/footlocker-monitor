import requests
from bs4 import BeautifulSoup
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta

def extract_data(url):
    # Send an HTTP GET request to the webpage
    #url = 'https://www.footlocker.com.vn/catalogsearch/result/?q=dunk+low+'
    response = requests.get(url)
    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
  
    # Find the element by its HTML tag, class, or ID
    element = soup.find('div', class_='my-class')

    # Extract the text from the element
    if element:
        text = element.text
        #print(text)
    # Find the 'href' attribute of the 'a' element
    product_list = []

    for i in range(1, 5):
        class_name = f'item product product-item {i}'
        item = soup.find('li', class_=class_name)
    #product_items = sop.finud_all('li', class_='item product product-item')
    #print(product_items)
        if item:
            product = {}
            
            # Get the product link
            product['link'] = item.find('a', class_='product-item-link')['href']          
            # Get the product name
            product['name'] = item.find('h2', class_='product-item-name').text.strip()           
            # Get the product price
            product['price'] = item.find('span', class_='price').text.strip()
            # Get the product image
            product['image'] = item.find('img', class_='product-image-photo')['src']

            product_list.append(product)

    product_df = pd.DataFrame(product_list)

    # save the DataFrame
    product_df.to_csv("flt_vn.csv")

    return product_df

webhook_url = 'https://discord.com/api/webhooks/1152301148452171826/jk1KLU2MRdFVA_7hLXuo5Wp1dc_iO_cbmgWlG2hhKFY2CsM5uFt8UmnqvUUPNjw_IoCa'
# Function to send a message to Discord webhook# Function to send an Embed object to Discord webhook
def send_embed_to_discord_webhook(embed):
    payload = {
        "embeds": [embed]
    }
    response = requests.post(webhook_url, json=payload)
    if response.status_code != 204:
        print(f"Failed to send Embed object to Discord webhook. Status Code: {response.status_code}")

# Create an Embed object for each row and send it to Discord
def send_products_as_embeds(webhook_url):
    product_df = extract_data()
    for index, row in product_df.iterrows():
        embed = {
            "title": row['name'],
            "description": f"**Price:** {row['price']}\n**Link:** {row['link']}",
            "url": row['link'],
            "image": {"url": row['image']}
        }
        send_embed_to_discord_webhook(embed, webhook_url)


default_args = {
    'owner': 'longmadlife',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 9),
    'email': ['airflow@hodinhlong.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    dag_id = 'FLT_VN',
    catchup = False,
    default_args=default_args,
    description='FLT_VN',
    schedule=timedelta(minutes=1),
    ) 

extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,  # Modify the extraction function to accept the URL as a parameter
         dag=dag,
    )
    
send_task = PythonOperator(
        task_id='send_products_as_embeds',
        python_callable=send_products_as_embeds,  # Modify the webhook function to accept the URL as a parameter
         dag=dag,
    )
    
    extract_task >> send_task
    tasks.append(extract_task)

# Set the task dependencies
for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]