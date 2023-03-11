from web3 import Web3
from functools import reduce
from multiprocessing import Pool, cpu_count
import pymongo
from time import sleep, time
from tqdm import tqdm
import json

co = pymongo.MongoClient('mongodb://localhost:27017/')['symbc']['events']
queueco = pymongo.MongoClient('mongodb://localhost:27017/')['symbc']['queue']
poolco = pymongo.MongoClient('mongodb://localhost:27017/')['symbc']['pools']
w3 = Web3(Web3.HTTPProvider("https://eth.public-rpc.com/"))

toBeApply = []

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

def to_token_address(hexBytes):
    return Web3.toChecksumAddress(Web3.toHex(hexBytes[-20:]))

def fetchLogs(args, output = True):
    L, R = args["fromBlock"], args["toBlock"]
    while True:
        try:
            st = time()
            ret =  w3.eth.get_logs(args)
            if(output): print(f"     fetched {len(ret)} infos from blocks ( {int(L, 16)} - {int(R, 16)} ) in {time() - st} s.")
            return ret
        except Exception as e:
            print(f"Error occurs when fetching block [{ int(L, 16) }, { int(R, 16) }]", e)
            if "429 Client Error" in str(e):
                sleep(2)
            continue

def processLogs(log):
    outs = ""
    rawdata = {}
    # with open('logs.txt', 'a') as f:
        # f.write(str(log) + '\n\n')

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

    elif log["topics"][0].hex() == topicsSign['poolcreated']:
        maxLiquidityPerTick = w3.eth.contract(Web3.toChecksumAddress("0x" + log["data"][-40:]), abi=json.load(open("abis/v3_pool.abi"))).functions.maxLiquidityPerTick().call()
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
        rawdata = {
            'type': '3',
            '_id': {'address': address},
            'token0': token0,
            'token1': token1,
            'fee': hexstring2int(log["topics"][3].hex()),
            'maxLiquidityPerTick': str(maxLiquidityPerTick),
            'tickSpacing': hexstring2int(log["data"][2:66]),
            'birthBlock': log["blockNumber"]
        }
        poolco.insert_one(rawdata)

    _ = log
    if(outs != ""):
        rawdata = {
            'blockNumber': _['blockNumber'],
            '_id' : {'blockNumber': _['blockNumber'], 'logIndex': _['logIndex']},
            'address': _['address'],
            'topics': list(map(lambda x: x.hex(), list(_['topics']))),
            'data': _['data'],
            'transactionHash': _['transactionHash'].hex(),
            'transactionIndex': _['transactionIndex'],
            'blockHash': _['blockHash'].hex(),
            'removed': _['removed'],
            'handledData': outs,
        }
        co.insert_one(rawdata)
        # queueco.insert_one(data) 要保证 Pool created events 和其他 events 同时进库，防止 uniswapv3-cpp 漏掉 event
        return rawdata

def fetchAllPoolHandle(L, R):
    events = fetchLogs({
        "address": Web3.toChecksumAddress("0x1F98431c8aD98523631AE4a59f267346ea31F984"),
        "fromBlock": hex(L),
        "toBlock": hex(R),
        "topics": [ topicsSign['poolcreated'] ]
    })
    print(f'-- {len(events)} Pools\' info fetched.')

    with Pool(processes=6) as _pool:
        mutiply_res = [_pool.apply_async(processLogs,  (_,)  )  for _ in events]
        for res in tqdm(mutiply_res):
           toBeApply.append(res.get())


def fetchEvents(L, R):
    events = []
    pools = list(poolco.find({ "birthBlock": { "$lte": R} }))
    print(f"\nFrom Block {L} to {R}, {len(pools)} uniswap-v3 pools in total.")
    with Pool(processes=6) as _pool:
        mutiply_res = [_pool.apply_async(fetchLogs,  ({
                                                        "address": pool['_id']["address"],
                                                        "fromBlock": hex(L),
                                                        "toBlock": hex(R)
                                                    }, False)  )  for pool in pools]
        for res in tqdm(mutiply_res):
            events.extend(res.get(timeout=500))
    return events

def getDbLatestBlock():
    ret = list( co.find({"address":{"$ne":"0x1F98431c8aD98523631AE4a59f267346ea31F984"}}).sort([("blockNumber", -1), ("logIndex", -1)]).limit(1) )
    retpool = list( co.find().sort([("_id.blockNumber", -1), ("_id.logIndex", -1)]).limit(1) )

    if len(ret) == 0:
        print('Not found any data in db.')
        poolco.create_index([('birthBlock', 1)])
        queueco.create_index([('blockNumber', 1)])
        return (12369611, 12369611)

    return (ret[0]['_id']['blockNumber'], retpool[0]['_id']['blockNumber'])

def getLatestBlock():
    while True:
        try:
            return w3.eth.get_block_number()
        except Exception as e:
            print(f'Error occured when fetching LatestBlock: {str(e)}')
            if "429 Client Error" in str(e):
                sleep(1)
            continue

if __name__ == '__main__':
    steplength = 3000
    while True:
        nowLatestBlock = getLatestBlock()
        (dbLatestBlock, dbLatestBlockForPool) = getDbLatestBlock()


        if(nowLatestBlock == dbLatestBlock):
            print('All Blocks have been synced in db.')
            sleep(0.5)
            continue
        print(f'+++++++++++++++++++++++++++++++++++++++++ Process events from block {dbLatestBlock + 1} to {nowLatestBlock} +++++++++++++++++++++++++++++++++++++++++')


        for i in range(dbLatestBlock + 1, nowLatestBlock + 1, steplength):
            L = i
            R = min(nowLatestBlock, i + steplength - 1)

            print(f"\n\n\n\n============================================= Processing blocks from {L} to {R} =============================================")

            fetchAllPoolHandle(L, R) # 看看我不在的日子里，链上又多了哪些新朋友 QwQ
            events = fetchEvents(L, R)
            for event in [processLogs(_) for _ in events]:
                if event:
                    toBeApply.append(event)

            toBeApply.sort(key= lambda x : (x["_id"]["blockNumber"], x["_id"]["logIndex"]))
            for data in tqdm(toBeApply):
                queueco.insert_one(data)

            print(f'++ {len(toBeApply)} events saved.')
            toBeApply = []


