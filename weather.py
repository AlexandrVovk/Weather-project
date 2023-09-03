import requests
import json
from boto3 import client
from os import environ
from sys import exit
import logging


profile = environ.get("AWS_PROFILE")
api_id = environ.get("API_ID")
dynamodb_table = environ.get("DYNAMODB_TABLE")
locations = environ.get("LOCATIONS")
url_base = 'https://api.openweathermap.org/data/2.5/weather'
url = f"{url_base}?q={locations}&units=metric&appid={api_id}"

def checks(response):
    if response.status_code != 200:
        exit(f"ERROR: the API resposponse code is: {response.status_code}")
    response_content = json.loads(response.content)
    if not response_content.get("main") and not response_content.get("dt"):
        exit(f"ERROR: the response cannot be parsed. Please check you parser.")
    return {"checks": True, "content": response_content}

def get_args(response):
    temp = round(response["content"]["main"]["temp"])
    date = round(response["content"]["dt"])
    location = locations.split(",")
    return {"temp": temp, "date": date, "location": location}

def put_dynamodb_item(location, date, temp):
    logging.info(f"location: {location[0]}")
    logging.info(f"temp: {temp}")
    logging.info(f"date: {date}")
    dynamodb_conn = client('dynamodb')

    put = dynamodb_conn.put_item(
        TableName=dynamodb_table,
        Item={
            "date_id": {"N": str(date)},
            "city": {"S": str(location[0])},
            "temp": {"N": str(temp)}
        },
        ReturnValues="ALL_OLD"
    )
    logging.info(f"put: {put}")

def main():
    log_format = '%(asctime)s %(levelname)-8s Weather: %(message)s'
    logging.basicConfig(level=logging.INFO,format=log_format)

    response = requests.get(url)
    response = checks(response)

    if response["checks"] == True:
        args = get_args(response)
        put_dynamodb_item(args["location"], args["date"], args["temp"])

if __name__ == "__main__":
    main()
