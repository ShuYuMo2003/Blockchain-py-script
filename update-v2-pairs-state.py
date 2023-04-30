import redis
from web3 import Web3
import json
from requests import post
from multiprocessing import Pool
from time import sleep
from random import choice
from tqdm import tqdm

Red = redis.Redis(host='192.168.80.128',port=8080,db=0)

NowBlock = int(Red.get('UpdatedToBlockNumber'))

commonRPCPointUri = [
    f"https://eth.public-rpc.com/",
]


commonRPCPointHandle = [Web3(Web3.HTTPProvider(Uri)) for Uri in commonRPCPointUri]

# def fetchV2PairsReserve(address, afterBlockNumber):
#     while True:
#         result = post(
#             choice(['https://mainnet.gateway.tenderly.co/2jRiGkCHOkLQ9BjEkbNhal',
#                     'https://mainnet.gateway.tenderly.co/Gt320SnBdHQu5SONf9ACG',
#                     'https://mainnet.gateway.tenderly.co/5FnLms9VT0rb4Ny1OUCLFK',
#                     'https://mainnet.gateway.tenderly.co/4fTQudixYa8ErxaXoftTuH',
#                     'https://mainnet.gateway.tenderly.co/5QC1CBB3O2OEzlVXqXXwrS']),
#             headers = { 'Content-Type': 'application/json' },
#             data = json.dumps({
#                 "id": 0,
#                 "jsonrpc": "2.0",
#                 "method": "tenderly_simulateTransaction",
#                 "params": [
#                     {
#                     "from": "0x0000000000000000000000000000000000000000",
#                     "to": str(address),
#                     "gas": "0x7a1200",
#                     "gasPrice": "0x0",
#                     "value": "0x0",
#                     "data": "0x0902f1ac"
#                     },
#                     hex(afterBlockNumber),
#                 ]
#             })
#         )
#         if('rate limit exceeded' in result.text):
#             print(address, result.text)
#             sleep(2)
#         else:
#             break

#     if(result):
#         output = json.loads(result.text)['result']['trace'][0]['output'][2:]
#         reserve0 = int(output[0:64], 16)
#         reserve1 = int(output[64:128], 16)
#         return (address, reserve0, reserve1)
#     else:
#         return (address, None, None)

# def getReserves(address, afterBlockNumber):
#     payload = open('payloadtemp.txt', 'r').read();
#     # payload = payload.replace('{{address}}', address).replace('{{block}}', str(afterBlockNumber))
#     print(payload)
#     resp = post('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2', data = payload)
#     print(resp.text)


ABIABI = json.load(open("abis/v2_pair.abi"))
def fetchV2PairsCurrentReserve(address, blocknumber):
    while True:
        try:
            ret = choice(commonRPCPointHandle).eth.contract(Web3.toChecksumAddress(address), abi=ABIABI).functions.getReserves().call(block_identifier=blocknumber)
            break
        except Exception as e:
            print(f"Fetch {address} Reserve error, Message: ", str(e))



    return [address, (int(ret[0])), (int(ret[1]))]


if __name__ == '__main__':
    # for i in range(16897494, 16897494 + 20):
    #     getReserves('0x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852', i)
    #     # print('after ', i, fetchV2PairsReserve('0x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852', i))


    AllowedTokens = open('get-available-tokens/whitelist.txt', 'r').read()
    RawAllPairs = set([i.decode('utf-8') for i in list(Red.hkeys('v2pairsData'))])
    AllPairs = []
    for pair in tqdm(RawAllPairs):
        ret = Red.hget('v2pairsData', pair)
        token0 = ret.decode('utf-8').split()[1]
        token1 = ret.decode('utf-8').split()[2]
        if token0 in AllowedTokens and token1 in AllowedTokens:
            AllPairs.append(pair)
    with open('AllowedPairs.txt', 'w') as f:
        f.write('\n'.join(AllPairs))
    cnt = 0


    command = []
    idd = 0
    with Pool(processes=4) as pool:
        result = [pool.apply_async(fetchV2PairsCurrentReserve, (address,NowBlock)) for address in AllPairs]

        for single in tqdm(result):
            ret = single.get()
            idd += 1
            Red.rpush('queue', f'2set {ret[0]} {ret[1]} {ret[2]} ' + str(NowBlock) + ('0' * (5 - len(str(idd)))) + str(idd))
            sss = f'2set {ret[0]} {ret[1]} {ret[2]} ' + str(NowBlock) + ('0' * (5 - len(str(idd)))) + str(idd)
            command.append(sss) 

    with open('Commands', 'w') as f:
        f.write('\n'.join(command))

    pass
