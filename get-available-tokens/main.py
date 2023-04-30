from requests import get


normal = get("https://tokens.uniswap.org/").json()
extra = get("https://extendedtokens.uniswap.org/").json()



result = []


for token in normal['tokens']:
    if(token['chainId'] == 1):
        result.append((token['name'], token['address']))

for token in extra['tokens']:
    if(token['chainId'] == 1):
        result.append((token['name'], token['address']))

whitelist = open("whitelist.txt", "w")

whitelist.write('\n'.join(map(lambda x : (' '.join([x[0].replace(' ', '-').replace('\t', '-'), x[1]])), result)))
whitelist.flush()
whitelist.close()