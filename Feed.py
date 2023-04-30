import redis
from tqdm import tqdm
Red = redis.Redis(host='192.168.80.128',port=8080,db=0)


events = open('eventsForPerTestFinal.txt', 'r').read().split('#')

for event in tqdm(events):
	Red.rpush('queue', event)


