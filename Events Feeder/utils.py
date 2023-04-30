import asyncio
import json
import requests
from websockets import connect
import web3
from time import sleep, time
from multiprocessing import Pool
from tqdm import tqdm

async def get_event(callback, wssurl):
    async with connect(wssurl) as ws:
        await ws.send('{"id": 1, "method": "eth_subscribe", "params": ["logs", {}]}')
        subscription_response = await ws.recv()
        print(subscription_response)
        while True:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=60)
                callback(json.loads(message))
            except Exception as e:
                print(e)


def EventsListener(callback, wssurl):
    while True:
        asyncio.run(get_event(callback, wssurl))

def getLatestBlock(w3):
    while True:
        try:
            return w3.eth.get_block_number()
        except Exception as e:
            print(f'Error occured when fetching LatestBlock: {str(e)}')
            if "429 Client Error" in str(e):
                sleep(1)
            continue

__RPC_URI = 'https://rpc.ankr.com/eth/4163a6afb3facd3e982c1d99cfe4ea9464ac1f19e4f5eab027ae3fb4e074e039'
__web3 = web3.Web3(web3.Web3.HTTPProvider(__RPC_URI))
def fetchLogs(blockL, blockR):
    while True:
        try:
            ret =  __web3.eth.get_logs({'fromBlock': hex(blockL), 'toBlock': hex(blockR)})
            return ret
        except Exception as e:
            print("fetchLogs", str(e))
            continue

__uniswapV2abi = json.loads(open('v2.abi', 'r').read())
def fetchV2PairReserve(w3, address, block):
    while True:
        try:
            ret = w3.eth.contract(w3.toChecksumAddress(address), abi=__uniswapV2abi).functions.getReserves().call(block_identifier=block)
            break
        except Exception as e:
            print(f"Fetch {address} Reserve error, Message: ", str(e))

    return [address, (int(ret[0])), (int(ret[1]))]


def getDbLatestBlock(db):
    while(not db.get("UpdatedToBlockNumber")):
        sleep(0.5)
        print('Not Found Data in db')

    return int(db.get("UpdatedToBlockNumber"))

def ProcessV2Event(w3, log):
    outs = f'2set ' + ' '.join(map(str, fetchV2PairReserve(w3, log['address'], log['blockNumber']))) + ' '
    return outs + str(log['blockNumber']) + '0' * (5 - len(str(log['logIndex']))) + str(log['logIndex']) # blocknumber + 5位长度 logindex

def ProcessV3Event(w3, log):
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

    topicsSign = {
        'initialize' : '0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95',
        'mint': '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde',
        'burn': '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c',
        'swap': '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
        'poolcreated': '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
    }

    outs = ""
    rawdata = {}

    if(len(log['topics']) < 1):
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


    _ = log
    if(outs != ""):
        rawdata = outs + str(_['blockNumber']) + '0' * (5 - len(str(_['logIndex']))) + str(_['logIndex']) # blocknumber + 5位长度 logindex
        return rawdata
    else:
        return ""

def fetchProcessHandle(blockL, blockR, v2Addresses, v3Addresses):
    logs = fetchLogs(blockL, blockR)
    result = []
    for event in logs:
        # if event['address'] in v2Addresses:
        #     v2e = ProcessV2Event(__web3, event)
        #     result.append(v2e)
        if event['address'] in v3Addresses:
            v3e = ProcessV3Event(__web3, event)
            result.append(v3e)
    return result

def UpdateV2Reserve(v2Addresses, block):
    event = []
    with Pool(processes=4) as pool:
        result = [pool.apply_async(fetchV2PairReserve, (address, block)) for address in v2Addresses]

        for single in tqdm(result):
            ret = single.get()
            idd += 1
            sss = f'2set {ret[0]} {ret[1]} {ret[2]} ' + str(block) + ('0' * (5 - len(str(idd)))) + str(idd)
            event.append(sss)
    return event