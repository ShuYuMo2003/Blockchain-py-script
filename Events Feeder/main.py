# import the following dependencies
import json
from web3 import Web3
import redis
from multiprocessing import Pool
from tqdm import tqdm

from threading import Thread, Lock

from utils import getLatestBlock, getDbLatestBlock, ProcessV3Event, ProcessV2Event, EventsListener, fetchLogs, fetchProcessHandle, UpdateV2Reserve


RPC_URI = 'https://rpc.ankr.com/eth/4163a6afb3facd3e982c1d99cfe4ea9464ac1f19e4f5eab027ae3fb4e074e039'
WSS_URI = 'wss://rpc.ankr.com/eth/ws/4163a6afb3facd3e982c1d99cfe4ea9464ac1f19e4f5eab027ae3fb4e074e039'
web3 = Web3(Web3.HTTPProvider(RPC_URI))

StepLen = 5

db = redis.Redis(host='192.168.80.128',port=8080,db=0)

v2Addresses = [i.decode('utf-8') for i in list(db.hkeys('v2pairsData'))]
v3Addresses = [i.decode('utf-8') for i in list(db.hkeys('v3poolsData'))]
conbinedAddresses = v2Addresses + v3Addresses

uniswapV2abi = json.loads(open('v2.abi', 'r').read())
uniswapV3abi = json.loads(open('v3.abi', 'r').read())

v2Contacts = [{address :  web3.eth.contract(address=address, abi=uniswapV2abi)} for address in v2Addresses]
v3Contacts = [{address :  web3.eth.contract(address=address, abi=uniswapV3abi)} for address in v3Addresses]

logs = open('events.txt', 'w')

def handle():
    def pushInto(e):
        pass
        #TODO: push into redis.
    EventsListener(pushInto, WSS_URI)


def main():
    CurrentBlock = getDbLatestBlock(db) + 1
    while True:
        print('current = ', CurrentBlock)
        if(CurrentBlock >= getLatestBlock(web3)):
            print('================= catch up with latest block! ======================')
            break
        else:
            NextBlock = min(CurrentBlock + 1000, getLatestBlock(web3))
            events = []
            print('fetching from ', CurrentBlock, ' to ', NextBlock, '...')
            with Pool(processes=4) as pool:
                multiply_mul = [pool.apply_async(fetchProcessHandle, (i, i + StepLen - 1, v2Addresses, v3Addresses)) for i in range(CurrentBlock, NextBlock + 1, StepLen)]
                for result in tqdm(multiply_mul):
                    events.extend(result.get())

            print('fetched ', len(events), ' events')

            for event in events:
                logs.write(event + '\n')
                logs.flush() #TODO write to redis

            CurrentBlock = NextBlock + 1
        if(getLatestBlock(web3) - CurrentBlock < 10):
            pass
            #TODO: set up listerner

    events = UpdateV2Reserve(v2Addresses, CurrentBlock)
    for event in events:
        logs.write(event + '\n')
        logs.flush() #TODO write to redis

if __name__ == "__main__":
    main()