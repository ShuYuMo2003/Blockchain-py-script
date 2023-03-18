# 12369728
# 10000835
from web3 import Web3
from time import time, sleep
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import json
w3 = Web3(Web3.HTTPProvider("https://eth.public-rpc.com/"))

def to_token_address(hexBytes):
    return Web3.toChecksumAddress(Web3.toHex(hexBytes[-20:]))

def fetchLogs(args, output = True):
    L, R = args["fromBlock"], args["toBlock"]
    while True:
        try:
            st = time()
            ret =  w3.eth.get_logs(args)
            # if(output): print(f"     fetched {len(ret)} infos from blocks ( {int(L, 16)} - {int(R, 16)} ) in {time() - st} s.")
            return ret
        except Exception as e:
            print(f"Error occurs when fetching block [{ int(L, 16) }, { int(R, 16) }]", e)
            if "429 Client Error" in str(e):
                sleep(2)
            continue

if __name__ == '__main__':
    with Pool(processes=10) as _pool:
        ret = []
        mutily_res = [ _pool.apply_async(fetchLogs, ({
                        "address": Web3.toChecksumAddress("0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"),
                        "fromBlock": hex(L),
                        "toBlock": hex(L + 3000 - 1),
                        "topics": [ '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9' ]
                    },)) for L in range(10000834, 12370728, 3000) ]
        for res in tqdm(mutily_res):
            for log in res.get():
                assert(log["topics"][0].hex() == '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9')
                address =  Web3.toChecksumAddress("0x" + log["data"][2:66][-40:])
                token0 = to_token_address(log["topics"][1])
                token1 = to_token_address(log["topics"][2])
                ret.append((address, token0, token1))
        with open("v2Pairs.json", 'w') as f:
            f.write(json.dumps(ret))
