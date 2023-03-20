from requests import post
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import json
from time import sleep
from random import choice

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

def slot0(address, afterBlockNumber):
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
                    "data": "0x3850c7bd"
                    },
                    hex(afterBlockNumber),
                ]
            })
        )
        if('rate limit exceeded' in result.text):
            # print(address, result.text)
            sleep(2)
        else:
            break

    if(result):
        output = json.loads(result.text)['result']['trace'][0]['output'][2:]
        reserve0 = hexstring2int(output[0:64])
        reserve1 = hexstring2int(output[64:128])
        return (address, reserve0, reserve1)
    else:
        return (address, None, None)


if __name__ == '__main__':
    for block in range(12644620, 12644700):
        print(block, slot0('0xcba27c8e7115b4eb50aa14999bc0866674a96ecb', block))
    pass