from web3 import Web3
import json
from multiprocessing import Queue, Pool, Process
import pymongo
import .config

w3 = Web3(Web3.HTTPProvider("https://eth.public-rpc.com"))

def fetch(fromBlock, toBlock, topic):
    print(f"fetching PoolCreated events from {fromBlock} to {toBlock}")
    while True:
        try:
            logs = w3.eth.get_logs({
                "address": Web3.toChecksumAddress("0x0e44cEb592AcFC5D3F09D996302eB4C499ff8c10"),
                "fromBlock": hex(fromBlock),
                "toBlock": hex(toBlock),
            })
        except Exception as e:
            print(e)
            continue
        return logs

