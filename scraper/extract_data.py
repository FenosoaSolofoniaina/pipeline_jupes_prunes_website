import os
import json
import time
from datetime import datetime
import requests
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from google.cloud import bigquery
from google.oauth2 import service_account



def load_data_from_json(file_path: str) -> Any :
    """
        file_path : str : the file where a json data will be loaded
    """

    with open(file_path, 'r') as file :
        data = json.load(fp=file)
        return data
    

def extract_links(page_config: Any) -> list :
    """

    """
    
    results = []
    base_url = page_config['main_url']
    url = base_url
    page = 1

    while True :
        print(f'[{datetime.now().isoformat(' ', timespec='seconds')}] Entering in the webpage : {url}')
        response = requests.get(url)
        page_parsed = BeautifulSoup(response.text, 'html.parser')
        elements = page_parsed.select(page_config['products']['selector'])
        if len(elements) :
            print(f'-> Got {len(elements)} elements')
            for el in elements :
                attribute = el[page_config['products']['attribute']]
                results.append(f'https://lesjupesdeprune.com{attribute.split("?")[0]}')
            page += 1
            url = f'{base_url}?page={page}'
        else :
            print(f'No elements found, aborting')
            break

    print(f'---> Total of links to visit : {len(results)}')
    return results


def call_api(url: str) -> Any :
    """
        Make a call api to the url: str
    """

    try :
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f'CALL API ERROR : an error occured when try to make a call api to the url : "{url}"')
        print(f'REQUESTS ERROR : {e}')


def preprocessing(variant: object) -> object :
    """
        Extract only the necessary desired values
    """

    return {
        'id' : variant['id'],
        'name' : variant['name'],
        'sku' : variant['sku'],
        'title' : variant['public_title'],
        'barcode' : variant['barcode'],
        'net_price' : variant['price'],
        'gross_price' : variant['compare_at_price'],
        'currency' : 'EUR',
        'stock' : variant['available'],
        'stock_quantity' : variant['inventory_quantity']
    }


def load_into_bq(data: pd.DataFrame,
                 service_account_fp: str,
                 project_id: str,
                 dataset: str,
                 table: str) -> None :
    """
        Send data to bigquery
    """

    credentials = service_account.Credentials.from_service_account_file(service_account_fp)
    client = bigquery.Client(project=project_id, credentials=credentials)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    table_id = f'{project_id}.{dataset}.{table}'
    job = client.load_table_from_dataframe(data,
                                           table_id,
                                           job_config=job_config)
    job.result()

    print(f'[{datetime.now().isoformat(' ', timespec='seconds')}] Data sent to Bigquery : check "{table_id}" to be sure')


def pipeline(config_path: str,
             service_account_fp: str,
             project_id: str,
             dataset: str,
             table: str) -> None :
    """
        PIPELINE SCRIPT
    """

    df = pd.DataFrame()
    configuration = load_data_from_json(file_path=config_path)
    links = extract_links(page_config=configuration)

    for i_link, link in enumerate(links) :
        api_response = call_api(f'{link}.js')
        product_variants = api_response['variants']
        print(f'[{datetime.now().isoformat(' ', timespec='seconds')}] Link {i_link + 1} / {len(links)} has {len(product_variants)} variants')

        for variant in product_variants :
            current_df = preprocessing(variant=variant)
            current_df = pd.DataFrame(current_df, index=[len(df) + 1])
            df = pd.concat([df, current_df], axis=0)

        time.sleep(0.5)

    print(f"[{datetime.now().isoformat(' ', timespec='seconds')}] Got data of dimension {df.shape}  ")

    load_into_bq(data=df,
                 service_account_fp=service_account_fp,
                 project_id=project_id,
                 dataset=dataset,
                 table=table)


def main() -> None :
    """
        MAIN FUNCTION
    """
    print('==================== PROGRAM STARTED ====================')

    load_dotenv()
    CONFIG_FP = os.path.join(os.getcwd(), 'json/configuration.json')
    SERVICE_ACCOUNT_FILE_PATH = os.getenv('SERVICE_ACCOUNT_FILE_PATH')
    GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
    BQ_DATASET = os.getenv('BQ_DATASET')
    BQ_TABLE = os.getenv('BQ_TABLE', f'products_{int(time.time())}')

    pipeline(config_path=CONFIG_FP,
             service_account_fp=SERVICE_ACCOUNT_FILE_PATH,
             project_id=GCP_PROJECT_ID,
             dataset=BQ_DATASET,
             table=BQ_TABLE)

    print('==================== PROGRAM FINISHED ====================')


# ======================================================================== #
# ================================== MAIN ================================ #
# ======================================================================== #
main()