import requests
import json
import boto3
import os
from sys import exit
import logging


profile = os.environ.get("AWS_PROFILE")
api_id = os.environ.get("API_ID")
dynamodb_table = os.environ.get("DYNAMODB_TABLE")
locations = os.environ.get("LOCATIONS")
url_base = 'https://api.openweathermap.org/data/2.5/weather'
url = f"{url_base}?q={locations}&units=metric&appid={api_id}"

def main():
    log_format = '%(asctime)s %(levelname)-8s Weather: %(message)s'
    logging.basicConfig(level=logging.INFO,format=log_format)

    response = requests.get(url)
    response_content = json.loads(response.content)

    temp = round(response_content["main"]["temp"])
    date = round(response_content["dt"])

    logging.info(f"temp: {temp}")
    logging.info(f"date: {date}")

    location = locations.split(",")
    logging.info(f"location: {location[0]}")

    client = boto3.client('dynamodb')

    put = client.put_item(
        TableName=dynamodb_table,
        Item={
            "date_id": {"N": str(date)},
            "city": {"S": str(location[0])},
            "temp": {"N": str(temp)}
        },
        ReturnValues="ALL_OLD"
    )
    logging.info(f"put: {put}")

if __name__ == "__main__":
    main()
