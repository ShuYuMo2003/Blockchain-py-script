# import the following dependencies
import json
from web3 import Web3
import redis
from multiprocessing import Pool

from time import sleep, time
from tqdm import tqdm

from queue import Queue

from threading import Thread

from utils import getLatestBlock, getDbLatestBlock, ProcessV3Event, ProcessV2Event, EventsListener, fetchProcessHandle, UpdateV2Reserve, feedDb, fetchLogs


RPC_URI = 'https://rpc.ankr.com/eth/4163a6afb3facd3e982c1d99cfe4ea9464ac1f19e4f5eab027ae3fb4e074e039'
WSS_URI = 'wss://rpc.ankr.com/eth/ws/4163a6afb3facd3e982c1d99cfe4ea9464ac1f19e4f5eab027ae3fb4e074e039'
web3 = Web3(Web3.HTTPProvider(RPC_URI))

StepLen = 7

db = redis.Redis(host='192.168.80.128',port=8080,db=0)

# print('loading v2 pairs')
v2Addresses = [i.decode('utf-8') for i in list(db.hkeys('v2pairsData'))]
# print('loading v3 pool')
v3Addresses = [i.decode('utf-8') for i in list(db.hkeys('v3poolsData'))]
conbinedAddresses = v2Addresses + v3Addresses

uniswapV2abi = json.loads(open('v2.abi', 'r').read())
uniswapV3abi = json.loads(open('v3.abi', 'r').read())



# print('establising v2 contacts')
# v2Contacts = [{address :  web3.eth.contract(address=address, abi=uniswapV2abi)} for address in v2Addresses]
# print('establising v3 contacts')
# v3Contacts = [{address :  web3.eth.contract(address=address, abi=uniswapV3abi)} for address in v3Addresses]


currentFetchedEvents = Queue(100000)

def fetchRange(blockL, blockR):
    print(f'pre fetch block events from {blockL} to {blockR}')
    with Pool(processes=4) as pool:
        multiply_mul = [pool.apply_async(fetchProcessHandle, (i, i + StepLen - 1, v2Addresses, v3Addresses)) for i in range(blockL, blockR + 1, StepLen)]
        for result in tqdm(multiply_mul):
            feedDb(result.get(), db)

LastBlock = -1

def Listener():
    global currentFetchedEvents, LastBlock

    def handleEvent(e):
        block = int(e['blockNumber'], 16)
        if block != LastBlock:
            events = []
            repeat = 0
            while len(events) < 1:
                events = fetchLogs(block, block)
                repeat += 1
                sleep(0.1)

            print('!! New Block built: ', block, ' events: ', len(events), ' repeat ', repeat, 'delay', repeat * 0.1, 's')

            for e in events:
                if e['address'] in v3Addresses:
                    event = ProcessV3Event(web3, e)
                    if(event):
                        print('\nfetched v3 event: ', event)
                        currentFetchedEvents.put(event)

                if e['address'] in v2Addresses:
                    event = ProcessV2Event(web3, e)
                    if(event):
                        print('\nfetched v2 event: ', event)
                        currentFetchedEvents.put(event)

            LastBlock = block


        # e['address'] = Web3.toChecksumAddress(e['address'])
        # # print(e['address'])
        # event = ''



    EventsListener(handleEvent, WSS_URI)

def main():


    dbBlock = getDbLatestBlock(db) + 1
    currentBlock = getLatestBlock(web3) + 2
    print(f'Current Block: {currentBlock}, DB Block: {dbBlock}')

    ListenerThread = Thread(target=Listener)
    ListenerThread.start()

    # ListenerThread.join()

    fetchRange(dbBlock, currentBlock)

    print('============================================= fetchDone. get start to update v2 state =============================================')
    feedDb(UpdateV2Reserve(v2Addresses, currentBlock), db)

    print('============================================= update v2 state done. get start to feed by events listener =============================================')

    while True:
        event = currentFetchedEvents.get()
        print('queue length =', currentFetchedEvents.qsize(), 'feeding event: ', event)
        feedDb([event], db)

if __name__ == "__main__":
    main()