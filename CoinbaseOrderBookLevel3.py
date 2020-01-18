"""
created : johnnyjohnnson (https://github.com/johnnyjohnnson)
Ursprung: Daniel Paquin (gdax/WebsocketClient.py --> https://github.com/danpaquin)

Modul downloaded das volle (Level3) OrderBook von pro.coinbase.com herunter
"""

from __future__ import print_function
import json, pprint, requests
import datetime as dt
import time as timeModule

from threading import Thread, Lock
from websocket import create_connection, WebSocketConnectionClosedException


class WebsocketClient():
    fileobj = None
    Thread_Lock = Lock()
    date = None
    hour = None
    
    def __init__(
            self, url='wss://ws-feed.pro.coinbase.com',
            #currently these are all available coins to download
            products=[
                    'XTZ-USD'  , 'BCH-BTC' , 'GNT-USDC', 'ZRX-USD'  , 'LTC-BTC', 'ALGO-USD',
                    'BCH-EUR'  , 'ETC-GBP' , 'BCH-GBP' , 'LINK-USD' , 'LTC-EUR', 'ETH-EUR',
                    'BTC-EUR'  , 'DASH-USD', 'ETH-BTC' , 'EOS-EUR'  , 'ETH-DAI', 'ETC-BTC',
                    'BCH-USD'  , 'LTC-USD' , 'DAI-USDC', 'BAT-ETH'  , 'XLM-BTC', 'ZRX-EUR',
                    'EOS-BTC'  , 'ATOM-BTC', 'DASH-BTC', 'REP-BTC'  , 'XRP-EUR', 'XTZ-BTC',
                    'MANA-USDC', 'ETH-USD' , 'XLM-EUR' , 'OXT-USD'  , 'XRP-BTC', 'DNT-USDC',
                    'BAT-USDC' , 'ETH-GBP' , 'ZEC-USDC', 'LOOM-USDC', 'LTC-GBP', 'EOS-USD',
                    'CVC-USDC' , 'XRP-USD' , 'BTC-GBP' , 'BTC-USD'  , 'ZRX-BTC', 'ETC-USD',
                    'ZEC-BTC'  , 'ETH-USDC', 'ATOM-USD', 'BTC-USDC' , 'ETC-EUR', 'LINK-ETH',
                    'REP-USD'  , 'XLM-USD'
                    ],
            message_type='subscribe',
            storagePath='C:\\Users\\someFolderName\\Coinbase\\ORDERBOOK\\'
            ):
        
        if storagePath.endswith('\\'):
            self.SpeicherPfad = storagePath
        else:
            self.SpeicherPfad = storagePath + '\\'
        self.url = url #self.url darf am Ende kein "/" stehen haben
        #
        if products == None:
            #products ist ein string
            products = requests.get('https://api.pro.coinbase.com/products').text
            products = products[ 1 : -1 ]
            products = products.replace('false', 'False').replace('true', 'True')
            #eval macht aus diesem string ein tuple mit lauter Dictionaries
            productsTuple = eval(products)
            #jedes Dictionary steht für ein product, also mit Schleife alle products in eine Liste bringen
            products = [dictionary['id'] for dictionary in productsTuple if dictionary['status'] == 'online']
        #
        self.products = products #self.products muss vom typ Liste sein
        self.type = message_type
        self.stop = False
        self.stopTheScript = False
        self.ws = None
        self.thread_listen = None
        self.thread_check_fileobj = None
        self.thread_create_Orderbook = None
        
        
    def Master_Start(self):
                
        def Master():
            while True:
                try:
                    if not self.thread_check_fileobj.is_alive() \
                    and not self.thread_listen.is_alive():
                        self.close()
                        self.start()
                    elif not self.thread_check_fileobj.is_alive() \
                    and self.thread_listen.is_alive() and self.stop:
                        #wenn der Fall eintritt hängt self.recv() im listen-Thread fest
                        #ergo: Websocket schliesen das es von vorn beginnen kann
                        self.ws.close()
                        self.thread_check_fileobj = Thread(
                                target=self.check_fileobj, name='check_fileobj'
                                )
                        self.thread_check_fileobj.start()
                        self._connect()
                    elif self.thread_check_fileobj.is_alive() \
                    and not self.thread_listen.is_alive() and not self.stop:
                        #noch keine Ahnung warum es passiert aber es ist bereits passiert
                        #das hier stellt den ersten Versuch dar das Problem zu lösen
                        error_message=[
                                'es trat folgender fehler auf:',
                                'komischerweise hat sich nur der listen-Thread beendet',
                                'der Rest ist einfach so weiter gelaufen...'
                                ]
                        self.stop = True
                        self.on_error(error_message)
                    elif self.stopTheScript:
                        self.on_error(['Downloading wird beendet ... !!!'])
                        self.stop = True
                        break
                except Exception as e:
                    time = dt.datetime.now()
                    error_message=[
                            'Exception raised im Master-Thread @{}'.format(time),
                            'Exception-type: {}'.format(type(e)),
                            'Exception-message: {}'.format(e)
                            ]
                    if isinstance(e, AttributeError):
                        #es wird AttributeError geraist, weil die Fkt. zum ersten Mal läuft
                        #also muss auch self.start() angeschmissen werden
                        self.start()
                    elif isinstance(e, WebSocketConnectionClosedException):
                        pass
                    else:
                        self.on_error(error_message)
        
        #Start des Master-Prozesses
        self.Master_Thread = Thread(target=Master, name='Master_Thread')
        self.Master_Thread.start()
    
    def start(self):
        
        def _go():
            self._connect()
            self._listen()
        
        self.stop = False
        self.thread_check_fileobj = Thread(target=self.check_fileobj, name='check_fileobj')
        self.thread_check_fileobj.start()
        self.thread_listen = Thread(target=_go, name='listen')
        self.thread_listen.start()
                
    def _connect(self):
        self.on_info(['_connect() läuft'])
        #es folgt Verbindungsaufbau zu Websocket
        #Verbindungsaufbau wird abgefangen, weil er crashen kann
        failing2connect = True
        while failing2connect:
            try:
                self.ws = create_connection(self.url, timeout=5)
            except Exception as e:
                time = dt.datetime.now()
                error_message=[
                        'Exception raised beim Verbindungsaufbau zu Coinbase @{}'.format(time),
                        'Exception-type: {}'.format(type(e)),
                        'Exception-message: {}'.format(e)
                        ]
                self.on_error(error_message)
            else:
                failing2connect = False
        #
        if self.type == 'subscribe':
            sub_params = {'type': 'subscribe', 'product_ids': self.products, 'channels':['full']}
        elif self.type == 'heartbeat':
            sub_params = {'type': 'heartbeat', 'on': True}
        self.ws.send(json.dumps(sub_params))
        #Prüfung ob Verbindung besteht
        if self.ws.connected:
            self.on_open()
            self.on_info([self.url])
            self.on_info(self.products)
            self.on_info(['Skript kann jederzeit durch Eingabe "wsClient.stopDownloading()" beendet werden!'])
        else:
            error_message = ['Keine Connection zu WebsocketFeed vorhanden']
            self.on_error(error_message)
        self.stop = False
        #zu guter Letzt: create Snapshot of the OrderBook als paralleler Prozess
        self.thread_create_Orderbook = Thread(target=self.create_OrderBook_Snapshot, name='create_Orderbook')
        self.thread_create_Orderbook.start()

    def _listen(self):
        while not self.stop:
            try:
                message = '{}\n'.format(self.ws.recv())
                with WebsocketClient.Thread_Lock:
                    WebsocketClient.fileobj.write(message)
            except Exception as e:
                time = dt.datetime.now()
                error_message=[
                        'Exception raised im Listen-Thread @{}'.format(time),
                        'Exception-type: {}'.format(type(e)),
                        'Exception-message: {}'.format(e),
                        'es folgt Neustart'
                        ]
                self.on_error(error_message)
                self.stop = True

    def close(self):
        if self.type == 'heartbeat':
            self.ws.send(json.dumps({'type': 'heartbeat', 'on': False}))
        self.on_close()
        self.stop = True
        try:
            self.ws.close()
        except Exception as e:
            time = dt.datetime.now()
            error_message = [
                    'Exception raised in close()-Funktion @{}'.format(time),
                    'Exception-type: {}'.format(type(e)),
                    'Exception-message: {}'.format(e)
                    ]
            self.on_error(error_message)

    def on_open(self):
        self.on_info(['-- Subscribed! --'])

    def on_close(self):
        self.on_info(['-- Socket Closed (absichtlich) --'])
    
    def check_fileobj(self):
        #
        try:
            self.create_fileobj(dt.datetime.now())
        except FileNotFoundError as error:
            self.raiseFileNotFoundError(error)
        except PermissionError as error:
            self.raisePermissionError(error)
        #
        while not self.stop:
            jetzt = dt.datetime.now()
            timedelta_day  = (jetzt.date() - WebsocketClient.date).days
            timedelta_hour = jetzt.hour - WebsocketClient.hour
            #Prüfen ob eine neue Stunde gestartet hat, wenn ja --> create_fileobj()
            if timedelta_hour != 0:
                #eine Stunde ist rum
                with WebsocketClient.Thread_Lock:
                    WebsocketClient.fileobj.close()
                    self.create_fileobj(jetzt)
            #es folgt die Check-Routine, ob tatsächlich überhaupt noch Daten empfangen werden
            #wenn dem so sein sollte wird dementsprechend reagiert
            try:
                filesize = int(WebsocketClient.fileobj.tell())
                if filesize > old_filesize or filesize < old_filesize:
                    #alles läuft normal und fileobj wächst stetig
                    #ODER
                    #es wurde ein neues fileobjekt erzeugt
                    old_filesize = filesize
                    old_time = jetzt
                else:
                    #das ist der Fehlerfall, es werden keine Daten mehr empfangen,
                    #demzufolge stagniert das Wachstum des fileobjekts
                    #wenn der Zustand länger als 15s andauert wird Thread beendet
                    if dt.timedelta.total_seconds(jetzt - old_time) > 15:
                        error_message = [
                                'es wurden 15s lang keine Daten empfangen... es folgt Neustart',
                                'passiert @{}'.format(jetzt)
                                ]
                        self.on_error(error_message)
                        self.stop = True
                        #nächste Zeile ist sicherheitshalber, sonst verlässt du diese Schleife u.U. nie wieder
                        old_time = dt.datetime.now()
                        break
            except Exception as e:
                if isinstance(e, NameError):
                    #die Funktion läuft zum ersten Mal, beim zweiten Durchlauf sollte alles gut sein!
                    #Fehler kommt weil old_filesize referenziert wird befor es assigned wird
                    old_filesize = filesize
                    #Fehler kommt weil old_time referenziert wird befor es assigned wird
                    #passiert wenn die Verbindung kurzzeitig ausfällt und dann wieder zurück kommt
                    old_time = jetzt
                else:
                    error_message = [
                            'Exception raised im check_fileobj-Thread @{}'.format(jetzt),
                            'Exception-type: {}'.format(type(e)),
                            'Exception-message: {}'.format(e),
                            'es folgt Neustart'
                            ]
                    self.on_error(error_message)
                    self.stop = True
                    #da es ein unerwarteter Fehler ist, breche ich den check_fileobj-Thread ab,
                    #weil es keinen Sinn macht diesen Fehler Ewigkeiten im Loop laufen zu lassen
                    break
        
    def create_fileobj(self, time):
        
        def create_fileobj_name(time):
            fileobj_name = 'FullChannel_GDAX_{:04}{:02}{:02}_{:02}hr'.format(
                    time.year, time.month, time.day, time.hour)
            return fileobj_name
        
        WebsocketClient.date = time.date()
        WebsocketClient.hour = time.hour
        WebsocketClient.fileobj = open(
                self.SpeicherPfad + '{}.json'.format(create_fileobj_name(time)),
                mode='a',
                newline='\n')
        self.on_info(['file-object created @ {}'.format(time)])
   
    def create_OrderBook_Snapshot(self):
        
        def create_snapshot_name(time,product):
            snapshot_name = 'Snapshot_GDAX_{}_{:04}{:02}{:02}_{:02}{:02}Uhr'.format(
                    product, time.year, time.month, time.day, time.hour, time.minute
                    )
            return snapshot_name
        def get_product_OrderBook(product, level):
            url='https://api.pro.coinbase.com/products/{}/book'.format(product)
            response = requests.get(url, params={'level': level}).json()
            return response
        
        time = dt.datetime.now()
        for product in self.products:
            failing = True
            Zaehler = 0
            max_attempts = 3
            while failing and Zaehler < max_attempts:
                try:
                    snapshot_orderbook = get_product_OrderBook(product, level=3)
                    with open(
                            self.SpeicherPfad + '{}.json'.format(create_snapshot_name(time, product)),
                            mode='w'
                            ) as File:
                        json.dump(snapshot_orderbook, File)
                    failing = False
                except Exception as e:
                    if isinstance(e, FileNotFoundError):
                        self.raiseFileNotFoundError(e)
                    elif isinstance(e, PermissionError):
                        self.raisePermissionError(e)
                    else:
                        error_message = [
                                'Exception raised im create_OrderBook-Thread @{}'.format(time),
                                'Exception-type: {}'.format(type(e)),
                                'Exception-message: {}'.format(e),
                                'es folgt Neustart'
                                ]
                        self.on_error(error_message)
                        Zaehler += 1
                        if Zaehler >= max_attempts:
                            message = ['create-OrderBook-Vorgang zum {}ten Mal gescheitert (-->{})'.format(Zaehler, product)]
                            self.on_info(message)
                            timeModule.sleep(1)
                            Zaehler = 0
    
    def stopDownloading(self):
        self.on_close()
        self.stopTheScript = True
        WebsocketClient.fileobj.close()
    
    def raiseFileNotFoundError(self, error):
        self.stopTheScript = True
        raise FileNotFoundError(
                "\n[Errno {}] {}: {}"
                "\nPlease choose a storage-path that exists (and where you have write-permission)"
                "\nthe storage-path {} does not exist!".format(
                        error.errno, error.strerror, error.filename, self.SpeicherPfad
                        )
                ) from None
    
    def raisePermissionError(self, error):
        self.stopTheScript = True
        raise PermissionError(
                "\n[Errno {}] {}: {}"
                "\nPlease choose a storage-path where you have write-permission"
                "\nYou don't have write-permission for path {}".format(
                        error.errno, error.strerror, error.filename, self.SpeicherPfad
                        )
                ) from None
    
    def on_info(self, e):
        pprint.pprint(e, width=132)

    def on_error(self, e):
        pprint.pprint(e)


if __name__ == '__main__':
    
    wsClient = WebsocketClient(
            #where do you want the downloaded OrderBook to be stored?
            storagePath='C:\\Python_Skripte\\01_GDAX_TestIng',
            
            #for which products do you want the OrderBook to be downloaded?
            #these 12 products represent the Top10 of most traded products on Coinbase
            #if you use None as argument, this script will try to download all products available
            # --> not recommended --> storage-size!!!
            products=[
                    'LTC-USD','LTC-EUR','LTC-BTC','ETH-USD','ETH-EUR','ETH-BTC',
                    'BTC-USD','BTC-GBP','BTC-EUR','BCH-USD','BCH-BTC','BCH-EUR'
                    ]
            )
    #this starts the script and keeps it alive, unless you call the stopDownloading-method(),
    #or you simply close the running script
    wsClient.Master_Start()
    
    #if you whisch to stop downloading the order book you can call the stopDownloading() method
    #wsClient.stopDownloading()

