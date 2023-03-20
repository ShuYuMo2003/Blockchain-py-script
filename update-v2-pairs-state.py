import redis
from web3 import Web3
import json
from requests import post
from multiprocessing import Pool
from time import sleep
from random import choice
from tqdm import tqdm

w3 = Web3(Web3.HTTPProvider("https://eth.public-rpc.com/"))
Red = redis.Redis(host='127.0.0.1',port=6379,db=0)

def fetchV2PairsReserve(address, afterBlockNumber):
    while True:
        result = post(
            choice(['https://mainnet.gateway.tenderly.co/2jRiGkCHOkLQ9BjEkbNhal',
                    'https://mainnet.gateway.tenderly.co/Gt320SnBdHQu5SONf9ACG',
                    'https://mainnet.gateway.tenderly.co/5FnLms9VT0rb4Ny1OUCLFK',
                    'https://mainnet.gateway.tenderly.co/4fTQudixYa8ErxaXoftTuH',
                    'https://mainnet.gateway.tenderly.co/5QC1CBB3O2OEzlVXqXXwrS']),
            headers = { 'Content-Type': 'application/json' },
            data = json.dumps({
                "id": 0,
                "jsonrpc": "2.0",
                "method": "tenderly_simulateTransaction",
                "params": [
                    {
                    "from": "0x0000000000000000000000000000000000000000",
                    "to": str(address),
                    "gas": "0x7a1200",
                    "gasPrice": "0x0",
                    "value": "0x0",
                    "data": "0x0902f1ac"
                    },
                    hex(afterBlockNumber),
                ]
            })
        )
        if('rate limit exceeded' in result.text):
            print(address, result.text)
            sleep(2)
        else:
            break

    if(result):
        output = json.loads(result.text)['result']['trace'][0]['output'][2:]
        reserve0 = int(output[0:64], 16)
        reserve1 = int(output[64:128], 16)
        return (address, reserve0, reserve1)
    else:
        return (address, None, None)

if __name__ == '__main__':
    NowBlock = int(Red.get('UpdatedToBlockNumber'))
    AllPairs = set([i.decode('utf-8') for i in list(Red.hkeys('v2pairsData'))])
    with Pool(processes=4) as pool:
        result = [pool.apply_async(fetchV2PairsReserve, (address, NowBlock)) for address in AllPairs]

        for single in tqdm(result):
            ret = single.get()
            assert(ret[1] and ret[2])
            # Red.rpush
            print(f'2set {ret[0]} {ret[1]} {ret[2]} ' + str(NowBlock) + '99999')


    pass
