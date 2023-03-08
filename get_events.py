from web3 import Web3
from functools import reduce
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import pymongo

poolAddress = '0x45dda9cb7c25131df268515131f647d726f50608'

co = pymongo.MongoClient('mongodb://localhost:27017/')['uniswap-events'][poolAddress]
w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))

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
    # print(log)
    e = { "txn": log["transactionHash"].hex() }
    # Initialize (uint160 sqrtPriceX96, int24 tick)
    # 0x3b67e703392e08f3a96d46d0c84055a2a87e5464648e18b07f73688bda2e36af
    if log["topics"][0].hex() == "0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95":
        outs += f"initialize 0x0 {hexstring2uint(log['data'][:66])} {hexstring2int(log['data'][66:130])}\n"

    # Mint (address sender, index_topic_1 address owner, index_topic_2 int24 tickLower, index_topic_3 int24 tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
    # 0x1b2950bceee666d7fdcdce5e84c943ed2cf996a6c32bf4aa02968371a53b6df1
    elif log["topics"][0].hex() == "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde":
        outs += " ".join([str(e) for e in ["mint",
            "0x" + log["data"][26:66],
            hexstring2int(log["topics"][2].hex()),
            hexstring2int(log["topics"][3].hex()),
            hexstring2uint(log["data"][66:130]),
            hexstring2uint(log["data"][130:194]),
            hexstring2uint(log["data"][194:258])]]) + "\n"

    # Swap (index_topic_1 address sender, index_topic_2 address recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
    # 0xbd65fdb21240bf51666343f26f7fc97ab2f26865a8981071d35b8b4124d4667d
    elif log["topics"][0].hex() == "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67":
        amount0 = hexstring2int(log["data"][2:66])
        amount1 = hexstring2int(log["data"][66:130])
        outs += " ".join([str(e) for e in["swap",
            "0x" + log["topics"][1].hex()[-40:],
            1 if amount0 > 0 else 0,
            amount0 if amount0 > 0 else amount1,
            hexstring2uint(log["data"][130:194]),
            amount0,
            amount1,
            hexstring2uint(log["data"][194:258]),
            hexstring2int(log["data"][258:322])]]) + "\n"

    # Burn (index_topic_1 address owner, index_topic_2 int24 tickLower, index_topic_3 int24 tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
    # 0x015656080d3410d5f4c83479a43ba0d876a83cc967766fd304aa6348d35024f0
    elif log["topics"][0].hex() == "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c":
        outs += " ".join([str(e) for e in [
            "burn",
            "0x" + log["topics"][1].hex()[-40:],
            hexstring2int(log["topics"][2].hex()),
            hexstring2int(log["topics"][3].hex()),
            hexstring2uint(log["data"][2:66]),
            hexstring2uint(log["data"][66:130]),
            hexstring2uint(log["data"][130:194])
        ]]) + "\n"
    else:
        print(log)
        outs = ""
    return outs

def fetch(L, R):
    print(f"fetching block [{L}, {R}]")
    while True:
        try:
            return w3.eth.get_logs({
                "address": Web3.toChecksumAddress(poolAddress),
                "fromBlock": hex(L),
                "toBlock": hex(R),
            })
            break
        except Exception as e:
            print(f"Error occurs when fetching block [{L}, {R}]", e)
            continue

if __name__ == '__main__':
    with Pool(processes=12) as pool:
        steplength = 3000
        mutiply_res = [pool.apply_async(fetch, (i, i + steplength - 1)) for i in range(22765962, 38658604 + 1, steplength)]
        for res in mutiply_res:
            for _ in res.get():
                datas = processLogs(_)

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
                })

