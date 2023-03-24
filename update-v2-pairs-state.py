import redis
from web3 import Web3
import json
from requests import post
from multiprocessing import Pool
from time import sleep
from random import choice
from tqdm import tqdm

w3 = Web3(Web3.HTTPProvider("https://eth.public-rpc.com/"))
Red = redis.Redis(host='192.168.80.128',port=8080,db=0)

commonRPCPointUri = [
    "https://eth.public-rpc.com/",
    "https://eth.public-rpc.com/",
    # "https://eth.llamarpc.com",
    # "https://uk.rpc.blxrbdn.com",
    # "https://virginia.rpc.blxrbdn.com",
    # "https://eth.rpc.blxrbdn.com",
    "https://ethereum.blockpi.network/v1/rpc/public",
    "https://eth-mainnet.nodereal.io/v1/1659dfb40aa24bbb8153a677b98064d7",
    "https://eth-mainnet.public.blastapi.io",
    # "https://rpc.builder0x69.io",
    "https://ethereum.publicnode.com",
    "https://eth-mainnet.rpcfast.com?api_key=xbhWBI1Wkguk8SNMu1bvvLurPGLXmgwYeC4S6g2H7WdwFigZSmPWVZRxrskEQwIf",
    "https://1rpc.io/eth",
    "https://rpc.flashbots.net",
    "https://rpc.payload.de",
    "https://api.zmok.io/mainnet/oaen6dy8ff6hju9k",
    "https://rpc.ankr.com/eth",
    "https://eth-rpc.gateway.pokt.network",
    "https://api.securerpc.com/v1",
    "https://cloudflare-eth.com",
    "https://endpoints.omniatech.io/v1/eth/mainnet/public",
    "https://beta-be.gashawk.io:3001/proxy/rpc",
    "https://eth.api.onfinality.io/public",
    # "https://rpc.coinsdo.com/eth",

]

commonRPCPointHandle = [Web3(Web3.HTTPProvider(Uri)) for Uri in commonRPCPointUri]

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

ABIABI = json.load(open("abis/v2_pair.abi"))
def fetchV2PairsCurrentReserve(address):
    while True:
        try: 
            ret = choice(commonRPCPointHandle).eth.contract(Web3.toChecksumAddress(address), abi=ABIABI).functions.getReserves().call()
            break
        except Exception as e:
            print(f"Fetch {address} Reserve error, Message: ", str(e))



    return [address, (int(ret[0])), (int(ret[1]))]

if __name__ == '__main__':
    print(fetchV2PairsReserve('0x7b6abc75cf6c8abe52e047e11240d1aa9ed784e3', 16897494))
    exit(0)
    NowBlock = int(Red.get('UpdatedToBlockNumber'))
    AllPairs = set([i.decode('utf-8') for i in list(Red.hkeys('v2pairsData'))])
    cnt = 0
    with Pool(processes=12) as pool:
        result = [pool.apply_async(fetchV2PairsCurrentReserve, (address,)) for address in AllPairs]

        for single in tqdm(result):
            ret = single.get()
            Red.rpush('queue', f'2set {ret[0]} {ret[1]} {ret[2]} ' + str(NowBlock) + '99999')


    pass
