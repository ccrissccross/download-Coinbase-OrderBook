# download-Coinbase-OrderBook

## what can you do with it?
this script downloads the full Level-3 Order Book in real-time from the Coinbase Trading platform https://pro.coinbase.com/, which is a digital currency exchange.

It enables you to maintain an accurate and up-to-date copy of the Coinbase exchange order book! It catches all errors that could potentially occur, allowing you to download the order book without stress and maintenance.

Just free up some disk-space, run the script and you are done.


## Requirements
```bash
pip install websocket-client
```
here is the link to the GitHub-page: https://github.com/websocket-client/websocket-client

## Usage
- download the "CoinbaseOrderBookLevel3.py" file
- choose a path on your disc where you want your order book to be stored
	--> storagePath='C:\\yourPath\\as_a_String'
- if you don't modify the storage path the script won't start and throw errors at you

 ▶ execute that file and you are done

```python

...

if __name__ == '__main__':
    
	wsClient = WebsocketClient(

		#where do you want the downloaded OrderBook to be stored?
		storagePath='C:\\yourPath\\as_a_String',

		#for which products do you want the OrderBook to be downloaded?
		#the following 12 products represent the Top10 of most traded products on Coinbase
		#if you use None as argument, this script will try to download all products available
		# --> not recommended --> storage-size!!!
		products=[
				'LTC-USD','LTC-EUR','LTC-BTC','ETH-USD','ETH-EUR','ETH-BTC',
				'BTC-USD','BTC-GBP','BTC-EUR','BCH-USD','BCH-BTC','BCH-EUR'
				]
		)

	#this starts the script and keeps it alive for ever
	wsClient.Master_Start()

	#if you whisch to stop downloading the order book you can call the stopDownloading() method
	#wsClient.stopDownloading()
```
