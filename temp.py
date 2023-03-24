import redis
from web3 import Web3
import json
from requests import post
from multiprocessing import Pool
from time import sleep
from random import choice
from tqdm import tqdm

Red = redis.Redis(host='192.168.80.128',port=8080,db=0)

AllPairs = set([i.decode('utf-8') for i in list(Red.hkeys('v2pairsData'))])

CorrectPairs = set([pair[0] for pair in json.loads(open('v2Pairs.json').read())])


error = 0
for pair in AllPairs:
	if(pair not in CorrectPairs):
		Red.hdel('v2pairsData', pair)
		error += 1

print(len(AllPairs), error)