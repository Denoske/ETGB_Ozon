import os
import requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
import threading
import clickhouse_connect
app = Flask(__name__)
class OzonAPI:
    def __init__(self, client_id, api_key):
        self.client_id = client_id
        self.api_key = api_key

    def get_etgb_data(self):
        url = 'https://api-seller.ozon.ru/v1/posting/global/etgb'

        headers = {
            'Content-Type': 'application/json',
            'Client-Id': self.client_id,
            'Api-Key': self.api_key
        }
        from_date = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        to_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        date_data = {
            'from': from_date,
            'to': to_date
        }
        payload = {
            'date': date_data
        }
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            response_json = response.json()
            client = clickhouse_connect.get_client(host='localhost', username='default', port=8123, password='')
            for i in range(len(response_json["result"])):
                client.command("SELECT * FROM ETGB WHERE posting_number = %s and date_of_creation = %s",
                                        (response_json["result"][i]["posting_number"], response_json["result"][i]["etgb"]["date"]))

        return response.json()

def delete_from_bd_clone():
    client = clickhouse_connect.get_client(host='localhost', username='default', port=8123, password='')
    client.command("OPTIMIZE TABLE ETGB FINAL DEDUPLICATE")
    return
@app.route('/process_data', methods=['GET'])
def process_data():
    ozon_api = OzonAPI("XXX", "XXX")
    etgb_data = ozon_api.get_etgb_data()
    #delete_from_bd_clone()                #optional
    return jsonify(etgb_data)


if __name__ == '__main__':
    threading.Thread(target=app.run, kwargs={'host': '127.0.0.1', 'port': 5000}).start()
