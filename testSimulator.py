import json
from requests import post

uri = 'https://mainnet.gateway.tenderly.co/2jRiGkCHOkLQ9BjEkbNhal'
header = { 'Content-Type': 'application/json' }
data = {
  "id": 0,
  "jsonrpc": "2.0",
  "method": "tenderly_simulateTransaction",
  "params": [
    {
      "from": "0x0000000000000000000000000000000000000000",
      "to": "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
      "gas": "0x7a1200",
      "gasPrice": "0x0",
      "value": "0x0",
      "data": "0x0902f1ac"
    },
    "0x1012f2b", # 16854827
  ]
}

# Factory: 0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f

def fetchV2PairsReserve(address, afterBlockNumber):
    result = post(
        'https://mainnet.gateway.tenderly.co/2jRiGkCHOkLQ9BjEkbNhal',
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
    if(result):
        outputs = json.loads(result.text)['result']['trace'][0]['decodedOutput']
        reserve0, reserve1 = 0, 0
        for output in outputs:
            if output['name'] == '_reserve0':
                reserve0 = output['value']
            if output['name'] == '_reserve1':
                reserve1 = output['value']
        return (reserve0, reserve1)
    else:
        return (None, None)


print(fetchV2PairsReserve('0xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11', 16855100))