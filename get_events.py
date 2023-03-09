from web3 import Web3
from functools import reduce
from multiprocessing import Pool, cpu_count
import pymongo
from time import sleep, time
import json

co = pymongo.MongoClient('mongodb://localhost:27017/')['bc']['events']
w3 = Web3(Web3.HTTPProvider("https://eth.public-rpc.com"))

topicsSign = {
    'initialize' : '0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95',
    'mint': '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde',
    'burn': '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c',
    'swap': '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
    'poolcreated': '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
}

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


def processLogs(log):
    outs = ""
    if(len(log['topics']) < 1):
        return ""

    with open('logs.txt', 'a') as f:
        f.write(str(log) + '\n\n')

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
        maxLiquidityPerTick = w3.eth.contract(Web3.toChecksumAddress("0x" + log["data"][-40:]), abi=json.load(open("abis/v3_pool.abi"))).functions.maxLiquidityPerTick().call()
        outs = " ".join([str(e) for e in [
            "poolcreated",
            log["topics"][1].hex(), # Token 0
            log["topics"][2].hex(), # Token 1
            hexstring2int(log["topics"][3].hex()), # fee
            hexstring2int(log["data"][2:66]), # tickSpacing
            maxLiquidityPerTick, # maxLiquidityPerTick
            log["data"][-40:], # poolAddress
        ]]) + "\n"

    insert_info = ""
    _ = log
    if(outs != ""):
        insert_info = co.insert_one({
            'address': _['address'],
            'topics': list(map(lambda x: x.hex(), list(_['topics']))),
            'data': _['data'],
            'blockNumber': _['blockNumber'],
            'transactionHash': _['transactionHash'].hex(),
            'transactionIndex': _['transactionIndex'],
            'blockHash': _['blockHash'].hex(),
            'logIndex': _['logIndex'],
            'removed': _['removed'],
            'handledData': outs,
        })

def fetch(L, R):
    while True:
        try:
            st = time()
            ret =  w3.eth.get_logs({
                "fromBlock": hex(L),
                "toBlock": hex(R)
            })
            print(f"fetched {len(ret)} infos from block [{L}, {R}] in {time() - st} s.")
            return ret
            break
        except Exception as e:
            print(f"Error occurs when fetching block [{L}, {R}]", e)
            continue

def getDbLatestBlock():
    ret = list( co.find().sort([("blockNumber", -1), ("logIndex", -1)]).limit(1) )

    if len(ret) == 0:
        print('Not found any data in db.')
        return 12369621

    return ret[0]['blockNumber']


if __name__ == '__main__':
    steplength = 40
    while True:
        nowLatestBlock = w3.eth.get_block_number()
        dbLatestBlock = getDbLatestBlock()

        if(nowLatestBlock == dbLatestBlock):
            print('All Blocks have been synced in db.')
            sleep(0.01)
            continue

        if(nowLatestBlock - dbLatestBlock > steplength):
            print(f'============= Fetching Block from {dbLatestBlock + 1} to {nowLatestBlock} ===================')
            with Pool(processes = 25) as pool:
                mutiply_res = [pool.apply_async(fetch, (i, i + steplength - 1)) for i in range(dbLatestBlock + 1, nowLatestBlock, steplength)]
                for res in mutiply_res:
                    print('recoverd')
                    [processLogs(_) for _ in res.get()]
        else:
            print(f'New Block {nowLatestBlock} occurred.')
            [processLogs(_) for _ in fetch(dbLatestBlock + 1, nowLatestBlock)]




