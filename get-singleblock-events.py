from web3 import Web3
from functools import reduce
from multiprocessing import Pool, cpu_count
from time import sleep, time
from tqdm import tqdm
import json
from random import randint
from requests import post
import redis
from random import choice
import random

w3 = Web3(Web3.HTTPProvider("https://rpc.ankr.com/eth/4163a6afb3facd3e982c1d99cfe4ea9464ac1f19e4f5eab027ae3fb4e074e039"))
Red = redis.Redis(host='192.168.80.128',port=8080,db=0)

BlackList = [
    '0x01113a97c0273f3C7A96d304D9A034992Ddf0D96',
    '0x5F1dddbf348aC2fbe22a163e30F99F9ECE3DD50a',
]

topicsSign = {
    'initialize' : '0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95',
    'mint': '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde',
    'burn': '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c',
    'swap': '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
    'poolcreated': '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
}

tendlyEthRPCPoint = ['https://mainnet.gateway.tenderly.co/2jRiGkCHOkLQ9BjEkbNhal',
                     'https://mainnet.gateway.tenderly.co/Gt320SnBdHQu5SONf9ACG',
                     'https://mainnet.gateway.tenderly.co/5FnLms9VT0rb4Ny1OUCLFK',
                     'https://mainnet.gateway.tenderly.co/4fTQudixYa8ErxaXoftTuH',
                     'https://mainnet.gateway.tenderly.co/5QC1CBB3O2OEzlVXqXXwrS']

commonRPCPointUri = [
    # "http://10.70.113.236:8551",
    "https://rpc.ankr.com/eth/4163a6afb3facd3e982c1d99cfe4ea9464ac1f19e4f5eab027ae3fb4e074e039",    
    # # "https://eth.public-rpc.com/",
    # # "https://eth.llamarpc.com",
    # # "https://uk.rpc.blxrbdn.com",
    # # "https://virginia.rpc.blxrbdn.com",
    # # "https://eth.rpc.blxrbdn.com",
    # "https://ethereum.blockpi.network/v1/rpc/public",    
    # # "https://eth-mainnet.nodereal.io/v1/1659dfb40aa24bbb8153a677b98064d7",
    # # "https://eth-mainnet.public.blastapi.io",
    # # "https://rpc.builder0x69.io",
    # # "https://ethereum.publicnode.com",
    # # "https://eth-mainnet.rpcfast.com?api_key=xbhWBI1Wkguk8SNMu1bvvLurPGLXmgwYeC4S6g2H7WdwFigZSmPWVZRxrskEQwIf",
    # # "https://1rpc.io/eth",
    # # "https://rpc.flashbots.net",
    # # "https://rpc.payload.de",
    # # "https://api.zmok.io/mainnet/oaen6dy8ff6hju9k",
    # "https://rpc.ankr.com/eth",   
    # # "https://eth-rpc.gateway.pokt.network",
    # # "https://api.securerpc.com/v1",
    # # "https://cloudflare-eth.com",
    # # "https://endpoints.omniatech.io/v1/eth/mainnet/public",
    # # "https://beta-be.gashawk.io:3001/proxy/rpc",
    # # "https://eth.api.onfinality.io/public",
    # # "https://rpc.coinsdo.com/eth",
    # # "https://eth.api.onfinality.io/public",
    # # "https://rpc.coinsdo.com/eth",
]

commonRPCPointHandle = [Web3(Web3.HTTPProvider(Uri)) for Uri in commonRPCPointUri]

def hexstring2uint(s):
    if s[:2] == "0x":
        s = s[2:]
    return int(s, 16)

def hexstring2int(s, len=256):
    len=256
    x = hexstring2uint(s)
    if x >= 2**(len - 1):
        x = x - 2**len
    return x

def to_token_address(hexBytes):
    return Web3.toChecksumAddress(Web3.toHex(hexBytes[-20:]))

def getDbLatestBlock():
    while(not Red.get("UpdatedToBlockNumber")):
        sleep(0.5)
        print('Not Found Data in db')

    return int(Red.get("UpdatedToBlockNumber"))


def getLatestBlock():
    while True:
        try:
            return w3.eth.get_block_number()
        except Exception as e:
            print(f'Error occured when fetching LatestBlock: {str(e)}')
            if "429 Client Error" in str(e):
                sleep(1)
            continue

def fetchV2PairsReserve(address, afterBlockNumber):
    while True:
        result = post(
            choice(tendlyEthRPCPoint),
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
            sleep(1)
        else:
            break

    assert(result)
    output = json.loads(result.text)['result']['trace'][0]['output'][2:]
    reserve0 = int(output[0:64], 16)
    reserve1 = int(output[64:128], 16)
    return (address, reserve0, reserve1)


def fetchLogs(args, output = True):
    L, R = args["fromBlock"], args["toBlock"]
    while True:
        try:
            st = time()
            ret =  choice(commonRPCPointHandle).eth.get_logs(args)
            delta = time() - st
            if(output): print(f"     fetched {len(ret)} infos from blocks ( {int(L, 16)} - {int(R, 16)} ) in {delta} s. rate = {len(ret) / delta}. ")
            return ret
        except Exception as e:
            print(str(e))
            # print(f"Error occurs when fetching block [{ int(L, 16) }, { int(R, 16) }]", e)
            # sleep(5 / (random.randint(2, 8)))
            # if "429 Client Error" in str(e):
                # sleep(2)
            continue

# 0x7d697d789ee19bc376474E0167BADe9535A28CF4
ABIABI = json.load(open("abis/v3_pool.abi"))
def GetMaxLiquidityPerTick(log):
    while True:
        try:
            return choice(commonRPCPointHandle).eth.contract(Web3.toChecksumAddress("0x" + log["data"][-40:]), abi=ABIABI).functions.maxLiquidityPerTick().call()
        except Exception as e:
            print(Web3.toChecksumAddress("0x" + log["data"][-40:]))
            print(e)
            pass

def ProcessV3Events(log):
    outs = ""
    rawdata = {}
    # with open('logs.txt', 'a') as f:
        # f.write(str(log) + '\n\n')

    if(len(log['topics']) < 1 or log['address'] in BlackList):
        return ""


    if log["topics"][0].hex() == topicsSign['initialize']:
        outs = f"initialize {log['address']} {hexstring2uint(log['data'][:66])} {hexstring2int(log['data'][66:130])}\n"
    elif log["topics"][0].hex() == topicsSign['mint']:
        outs = " ".join([str(e) for e in ["mint",
            log['address'],
            hexstring2int(log["topics"][2].hex()),
            hexstring2int(log["topics"][3].hex()),
            hexstring2uint(log["data"][66:130]),
            hexstring2uint(log["data"][130:194]),
            hexstring2uint(log["data"][194:258])]]) + "\n"

    elif log["topics"][0].hex() == topicsSign['swap']:
        amount0 = hexstring2int(log["data"][2:66])
        amount1 = hexstring2int(log["data"][66:130])
        outs = " ".join([str(e) for e in["swap",
            log['address'],
            1 if amount0 > 0 else 0,
            amount0 if amount0 > 0 else amount1,
            hexstring2uint(log["data"][130:194]),
            amount0,
            amount1,
            hexstring2uint(log["data"][194:258]),
            hexstring2int(log["data"][258:322])]]) + "\n"

    elif log["topics"][0].hex() == topicsSign['burn']:
        outs = " ".join([str(e) for e in [
            "burn",
            log['address'],
            hexstring2int(log["topics"][2].hex()),
            hexstring2int(log["topics"][3].hex()),
            hexstring2uint(log["data"][2:66]),
            hexstring2uint(log["data"][66:130]),
            hexstring2uint(log["data"][130:194])
        ]]) + "\n"

    elif log["topics"][0].hex() == topicsSign['poolcreated']:
        maxLiquidityPerTick = GetMaxLiquidityPerTick(log)
        address =  Web3.toChecksumAddress("0x" + log["data"][-40:])
        token0 = to_token_address(log["topics"][1])
        token1 = to_token_address(log["topics"][2])
        outs = " ".join([str(e) for e in [
            "poolcreated",
            token0, # Token 0
            token1, # Token 1
            hexstring2int(log["topics"][3].hex()), # fee
            hexstring2int(log["data"][2:66]), # tickSpacing
            maxLiquidityPerTick, # maxLiquidityPerTick
            address, # poolAddress
        ]]) + "\n"


    _ = log
    if(outs != ""):
        rawdata = outs + str(_['blockNumber']) + '0' * (5 - len(str(_['logIndex']))) + str(_['logIndex']) # blocknumber + 5位长度 logindex
        return rawdata

ToBeUpdateAddress = set()
def ProcessV2Events(log):
    if(len(log["topics"]) < 1 or log['address'] in BlackList): return None
    # global ListeningPairs
    idxHash = str(log['blockNumber']) + '0' * (5 - len(str(log['logIndex']))) + str(log['logIndex'])
    if(log["topics"][0].hex() == '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'):
        address =  Web3.toChecksumAddress("0x" + log["data"][2:66][-40:])
        token0 = to_token_address(log["topics"][1])
        token1 = to_token_address(log["topics"][2])
        # ListeningPairs.add(address)
        return f'2create {address} {token0} {token1} {idxHash}'
    else:
        ToBeUpdateAddress.add(log['address'])
#        rev0, rev1 = fetchV2PairsReserve(address, blockNumber)
#        return " ".join([ '2set', address, str(rev0), str(rev1)]) + "\n" + str(log['blockNumber']) + '0' * (5 - len(str(log['logIndex']))) + str(log['logIndex'])


def PushRangeBlock(currentHandleBlockNumber, NextHandleBlockNumber, _Address):
    st = time()
    events = fetchLogs({"fromBlock": hex(currentHandleBlockNumber), "toBlock": hex(NextHandleBlockNumber - 1)}, False)

    # process v3 events
    v3e = []
    for event in events:
        if(event['address'] not in _Address): continue
        _ = ProcessV3Events(event)
        if _: v3e.append(_)

    # process v2 events
    # ToBeUpdateAddress = set()
    v2e = []
    for event in events:
        if(event['address'] not in _Address): continue
        _ = ProcessV2Events(event)
        if _: v2e.append(_)
    # ToBeUpdatePairs = ToBeUpdateAddress & ListeningPairs
    # print(f'The state of {len(ToBeUpdatePairs)} pairs changed. fetching new state:')
    # with Pool(processes=2) as pool:
    #     mulres = [pool.apply_async(fetchV2PairsReserve, (pair, NextHandleBlockNumber - 1)) for pair in ToBeUpdatePairs]
    #     for _ in tqdm(mulres):
    #         result = _.get()
    #         v2e.append(f'2set {result[0]} {result[1]} {result[2]} {NextHandleBlockNumber - 1}99999')

    processedEvent = v2e + v3e
    processedEvent.sort(key=lambda x : x.split()[-1])
    return (processedEvent, len(processedEvent) / (time() - st))

def fetchAllPoolNPair(L, R):
    Address = set()
    steplen = 1024
    with Pool(processes=12) as pool:
        mulret = [ pool.apply_async(fetchLogs, ({"fromBlock": hex(i), "toBlock": hex(i + steplen - 1),"address": Web3.toChecksumAddress("0x1F98431c8aD98523631AE4a59f267346ea31F984"), "topics": ["0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"]}, False)) for i in range(L, R, steplen)]
        print('fetch v3 pool')
        for _ in tqdm(mulret):
            logs = _.get()
            for log in logs:
                address =  Web3.toChecksumAddress("0x" + log["data"][-40:])
                Address.add(address)
        print('fetch v2 pool')
        mulret = [ pool.apply_async(fetchLogs, ({"fromBlock": hex(i), "toBlock": hex(i + steplen - 1),"address": Web3.toChecksumAddress("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"), "topics": ["0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"]}, False)) for i in range(L, R, steplen)]
        for _ in tqdm(mulret):
            logs = _.get()
            for log in logs:
                address =  Web3.toChecksumAddress("0x" + log["data"][2:66][-40:])
                Address.add(address)
    return Address

def testPRCS():
    for uri in commonRPCPointUri:
        w3 = Web3(Web3.HTTPProvider(uri))
        st = time()
        msg = ""
        try:
            ret = w3.eth.get_logs({"fromBlock": 12782000, "toBlock": 12782010})
        except Exception as e:
            msg = str(e)
        delta = (time() - st)
        print(f'{uri[7:15]}\t{delta}\t{msg}')
    pass

eventsHandle = open('eventsForPertest.txt', 'w')

if __name__ == '__main__':
    testPRCS()
    steplen = 13
    startPoint = getDbLatestBlock() + 1
    while True:
        endPoint   = getLatestBlock()
        if(startPoint > endPoint):
            print(f'{startPoint} 还没出现哦 QAQ')
            sleep(1)

        # ListeningPairs = set([i.decode('utf-8') for i in list(Red.hkeys("v2pairsData"))])
        # ListeningPools = set([i.decode('utf-8') for i in list(Red.hkeys("v3poolsData"))])

        ListeningAddress = set([i.decode('utf-8') for i in list(Red.hkeys("v2pairsData"))]) | set([i.decode('utf-8') for i in list(Red.hkeys("v3poolsData"))])
        print(f'All pool/pair cnt = {len(ListeningAddress)}')
        dd = fetchAllPoolNPair(startPoint, endPoint)
        ListeningAddress = ListeningAddress | dd
        ListeningAddress.add(Web3.toChecksumAddress("0x1F98431c8aD98523631AE4a59f267346ea31F984"))
        ListeningAddress.add(Web3.toChecksumAddress("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"))
        print(f'All pool/pair cnt = {len(ListeningAddress)}')

        with Pool(processes=8) as pool:
            mulret = [pool.apply_async(PushRangeBlock, (currentHandleBlockNumber, currentHandleBlockNumber + steplen, ListeningAddress)) for currentHandleBlockNumber in range(startPoint, endPoint, steplen)]
            with tqdm(mulret) as t:
                for _ in t:
                    tot = 0
                    ret, rate = _.get()

                    L = randint(1, len(ret) - 2)

                    part0 = ret[0:L]
                    part1 = ret[L:]
                    timestamp = part0[-1].replace('\n', ' ').split()[-1]
                    resultEvents = part0 + part1# + part1
                    # print('============================\n\n', resultEvents)
                    for event in resultEvents:
                        # eventsHandle.write(event + '#')
                        # eventsHandle.flush()
                        Red.rpush('queue', event)
                        tot += 1
                    t.set_description(f'saved {tot} events. rate = {rate} e/s')
        startPoint = endPoint + 1