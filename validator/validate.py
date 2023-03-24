from web3 import Web3
import json
from requests import post
from time import sleep
from random import choice
from tqdm import tqdm

simulatorUris = [
    'https://mainnet.gateway.tenderly.co/2jRiGkCHOkLQ9BjEkbNhal',
    'https://mainnet.gateway.tenderly.co/Gt320SnBdHQu5SONf9ACG',
    'https://mainnet.gateway.tenderly.co/5FnLms9VT0rb4Ny1OUCLFK',
    'https://mainnet.gateway.tenderly.co/4fTQudixYa8ErxaXoftTuH',
    'https://mainnet.gateway.tenderly.co/5QC1CBB3O2OEzlVXqXXwrS'
]

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

def Meet64(x):
    return '0' * (64 - len(str(x))) + str(x)

def V2reserve(address, afterBlockNumber):
    while True:
        result = post(
            choice(simulatorUris),
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

def getAmountOut(amountIn, reserve0, reserve1):
    while True:
        result = post(
            choice(simulatorUris),
            headers = { 'Content-Type': 'application/json' },
            data = json.dumps({
                "id": 0,
                "jsonrpc": "2.0",
                "method": "tenderly_simulateTransaction",
                "params": [
                    {
                    "from": "0x0000000000000000000000000000000000000000",
                    "to": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
                    "gas": "0x9a1200",
                    "gasPrice": "0x0",
                    "value": "0x0",
                    "data": f"0x054d50d4{Meet64(hex(amountIn)[2:])}{Meet64(hex(reserve0)[2:])}{Meet64(hex(reserve1)[2:])}"
                    },
                    'latest',
                ]
            })
        )
        if('rate limit exceeded' in result.text):
            # print(address, result.text)
            sleep(2)
        else:
            break
    assert(result)
    if(result):
        output = json.loads(result.text)['result']['trace'][0]['output'][2:]
        return hexstring2int(output)


def SwapV3(address, amount, zeroToOne, afterBlockNumber):
    fakeSender = '1111111254fb6c44bac0bed2854e76f90643097d'
    payload = json.dumps({
      "id": 0,
      "jsonrpc": "2.0",
      "method": "tenderly_simulateTransaction",
      "params": [
        {
          "from": "0xc64a950ab387b486ecb3711d9c79d4adc0f0f366",
          "to": address,
          "gas": "0x9a1200",
          "gasPrice": "0x0",
          "value": "0x0",
          "data": f"0x128acb08000000000000000000000000{fakeSender}{ Meet64(1 if zeroToOne else 0) }{ Meet64(hex(amount)[2:]) }{Meet64(hex(4295128740)[2:] if zeroToOne else hex(1461446703485210103287273052203988822378723970341)[2:] )}00000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000"
        },
        hex(afterBlockNumber)
      ]
    })
    # print(payload)
    while True:
        result = post(
            choice(simulatorUris),
            headers = { 'Content-Type': 'application/json' },
            data = payload
        )
        if('rate limit exceeded' in result.text):
            # print(address, result.text)
            sleep(2)
        else:
            break

    assert(result)
    # print(result.text)
    functions = json.loads(result.text)['result']['trace']

    for function in functions:
        try:
            if '0xa9059cbb' in function['input'] and fakeSender in function['input']:
                return hexstring2int(function['input'][-64:])
        except Exception as e:
            pass
    return None

def SwapV2(address, amount, zeroToOne, afterBlockNumber):
    (__, rev0, rev1) = V2reserve(address, afterBlockNumber)
    if(zeroToOne):
        return getAmountOut(amount, rev0, rev1)
    else:
        return getAmountOut(amount, rev1, rev0)


if __name__ == '__main__':
    amount = int(5.15363e+18)
    init = amount
    block = 16898272
    with open('data.txt', 'r') as f:
        for _raw_ in f.readlines():
            raw = _raw_.split()
            address = raw[1][:-3]
            zeroToOne = True if  raw[1][-3:] == '(1)' else False
            if(raw[0] == 'v2'):
                # print(f'V2 : {address} {amount} {zeroToOne} {block}', end='')
                afteramount = SwapV2(address, amount, zeroToOne, block)
            elif(raw[0] == 'v3'):
                # print(f'V3 : {address} {amount} {zeroToOne} {block}', end='')
                afteramount = SwapV3(address, amount, zeroToOne, block)
            else: assert(False)
            # print(' = ', afteramount)

            print(f'{raw[0]} {raw[1]} {amount} -> {afteramount}')
            amount = afteramount
        
    print('revenue ', (amount - init) / 1e18)


        
