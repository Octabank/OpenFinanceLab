#!/usr/bin/python

import requests
from datetime import datetime, date
import json
import pandas as pd
from IPython.display import clear_output, display

# API docs in https://www.mercadobitcoin.com.br/api-doc/

def download_btc_from_mbtc():
	api = "https://www.mercadobitcoin.net/api/BTC/day-summary/"

	day = date.today()
	timedelta = day - date(day.year, day.month, day.day-1)

	btc_json = {'date': [], 'opening': [], 'closing': [], 'lowest': [], 'highest': [], 
					'volume': [], 'quantity': [], 'amount': [], 'avg_price': []}

	while True:
		clear_output(wait=True)
		
		day -= timedelta
		url = api + str(day.year) + '/' + str(day.month) + '/' + str(day.day)
		print(url)
		
		r = requests.get(url)
		json = r.json()
		if r.status_code != 200:
			print('Download Completed')
			break
		try:
			json['error']
			print('Download Completed')
			break
		except:
			pass
			
		for key in json:
			btc_json[key].append(json[key])
		

	btc = pd.DataFrame.from_dict(btc_json)
	btc.to_csv('btcbrl.csv',index=False)