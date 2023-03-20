from web3 import Web3
from functools import reduce
from multiprocessing import Pool, cpu_count
from time import sleep, time
from tqdm import tqdm
import json
from requests import post
import redis

w3 = Web3(Web3.HTTPProvider("https://eth.public-rpc.com/"))
Red = redis.Redis(host='127.0.0.1',port=6379,db=0)

toBeApply = []
AllListingPool = set()
AllListingPair = set()

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

def fetchV2PairsReserve(address, afterBlockNumber):
    result = post(
        'https://mainnet.gateway.tenderly.co/2jRiGkCHOkLQ9BjEkbNhal',
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
    if(result):
        print(result.text)
        outputs = json.loads(result.text)['result']['trace'][0]['decodedOutput']
        reserve0, reserve1 = 0, 0
        for output in outputs:
            if output['name'] == '_reserve0':
                reserve0 = output['value']
            if output['name'] == '_reserve1':
                reserve1 = output['value']
        return (reserve0, reserve1)
    else:
        return (None, None)

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

    _ = log
    if(outs != ""):
        rawdata = outs + str(_['blockNumber']) + '0' * (5 - len(str(_['logIndex']))) + str(_['logIndex']) # blocknumber + 5位长度 logindex
        return rawdata

def ProcessV2Events(log):
    address = log['address']
    blockNumber = log['blockNumber']
    rev0, rev1 = fetchV2PairsReserve(address, blockNumber)
    return " ".join([ '2set', address, str(rev0), str(rev1)]) + "\n" + str(log['blockNumber']) + '0' * (5 - len(str(log['logIndex']))) + str(log['logIndex'])

def fetchAllPoolHandle(L, R):
    global toBeApply
    events = fetchLogs({
        "address": Web3.toChecksumAddress("0x1F98431c8aD98523631AE4a59f267346ea31F984"),
        "fromBlock": hex(L),
        "toBlock": hex(R),
        "topics": [ topicsSign['poolcreated'] ]
    })
    print(f'-- {len(events)} Pools\' info fetched.')

    addresses = []
    with Pool(processes=15) as _pool:
        mutiply_res = [_pool.apply_async(processLogs,  (_,)  )  for _ in events]
        for res in tqdm(mutiply_res):
           result = res.get()
           assert(result)
           toBeApply.append(result)
           addresses.append(str(result.split()[-2]))
    return addresses

def fetchAllPairHandle(L, R):
    global toBeApply
    events = fetchLogs({
        "address": Web3.toChecksumAddress("0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"),
        "fromBlock": hex(L),
        "toBlock": hex(R),
        "topics": [ '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9' ]
    })
    print(f'\n-- {len(events)} Pairs\' info fetched.')
    pairs = []
    for log in events:
        address =  Web3.toChecksumAddress("0x" + log["data"][2:66][-40:])
        token0 = to_token_address(log["topics"][1])
        token1 = to_token_address(log["topics"][2])
        idxHash = str(log['blockNumber']) + '0' * (5 - len(str(log['logIndex']))) + str(log['logIndex'])
        pairs.append((address, token0, token1, idxHash))

    for pair in pairs:
        toBeApply.append(f'2create {pair[0]} {pair[1]} {pair[2]} {pair[3]}')

    return [pair[0] for pair in pairs]


def fetchEvents(L, R):
    global AllListingPool
    events = []
    print(f"\nFrom Block {L} to {R}, {len(AllListingPool)} uniswap-v3 pools in total.")
    with Pool(processes=15) as _pool:
        mutiply_res = [_pool.apply_async(fetchLogs,  ({
                                                        "address": pool,
                                                        "fromBlock": hex(L),
                                                        "toBlock": hex(R)
                                                    }, False)  )  for pool in AllListingPool]
        for res in tqdm(mutiply_res):
            events.extend(res.get(timeout=500))
    return events


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

if __name__ == '__main__':
    steplength = 3000

    AllListingPool = set([i.decode('utf-8') for i in list(Red.hkeys("v3poolsData"))])
    AllListingPair = set([i.decode('utf-8') for i in list(Red.hkeys("v2pairsData"))])
    nowLatestBlock = getLatestBlock()
    dbLatestBlock = getDbLatestBlock()
    if dbLatestBlock == 12369728:
        ps = json.loads(open('v2Pairs.json', 'r').read())
        toBeApply = [f'2create {i[0]} {i[1]} {i[2]} 1236972800000' for i in ps]
        [AllListingPair.add(i[0]) for i in ps]

    print(f'+++++++++++++++++++++++++++++++++++++++++ Process events from block {dbLatestBlock + 1} to {nowLatestBlock} +++++++++++++++++++++++++++++++++++++++++')


    for i in range(dbLatestBlock + 1, nowLatestBlock + 1, steplength):
        L = i
        R = min(nowLatestBlock, i + steplength - 1)

        print(f"\n\n\n\n============================================= Processing blocks from {L} to {R} =============================================")

        NewFriend = fetchAllPoolHandle(L, R) # 看看我不在的日子里，链上又多了哪些新朋友 QwQ
        New2Friend = fetchAllPairHandle(L, R)

        [AllListingPool.add(i) for i in NewFriend]
        [AllListingPair.add(i) for i in New2Friend]

        events = fetchEvents(L, R)
        for event in [processLogs(_) for _ in events]:
            if event:
                toBeApply.append(event)

        toBeApply.sort(key= lambda x : (x.split()[-1]))
        for data in tqdm(toBeApply):
            Red.rpush('queue', data)

        print(f'++ {len(toBeApply)} events saved.')
        toBeApply = []


