import redis
from tqdm import tqdm

Red = redis.Redis(host='192.168.80.128',port=8080,db=0)


AllowedTokens = open('whitelist.txt', 'r').read()


RawAllPairs = set([i.decode('utf-8') for i in list(Red.hkeys('v2pairsData'))])
for pair in tqdm(RawAllPairs):
    ret = Red.hget('v2pairsData', pair)
    token0 = ret.decode('utf-8').split()[1]
    token1 = ret.decode('utf-8').split()[2]
    if (token0 not in AllowedTokens) or (token1 not in AllowedTokens):
    	Red.hdel('v2pairsData', pair)


RawAllPairs = set([i.decode('utf-8') for i in list(Red.hkeys('v3poolsInfo'))])
for pair in tqdm(RawAllPairs):
    ret = Red.hget('v3poolsInfo', pair)
    token0 = ret.decode('utf-8').split()[1]
    token1 = ret.decode('utf-8').split()[2]
    if (token0 not in AllowedTokens) or (token1 not in AllowedTokens):
    	Red.hdel('v3poolsInfo', pair)
    	Red.hdel('v3poolsData', pair)