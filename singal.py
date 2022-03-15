
import asyncio
import async_timeout
import aioredis
import aiohttp
import json
from datetime import datetime

configures = {}

async def send_signal(data: dict) -> (dict, bool):
    print('时间:', datetime.now(), data['side'], data['symbol'])
    headers = {"Content-Type": "application/json; charset=utf-8"}

    async with aiohttp.ClientSession() as session:
      async with session.post(configures['url'], headers=headers, data=json.dumps(data)) as resp:
        #   print("resp:", resp)
          print('时间:', datetime.now(), "resp:", resp.status)
          text = await resp.json()
          print("resp:", text)
        
    print('****')

async def handle_singal(message_str):
    message = json.loads(message_str)
    print(message)
    symbol = message["symbol"]
    symbol = symbol.replace('/', '')
    
    signal_data = configures['data']
    signal_data['side'] = message['action']
    signal_data['symbol'] = symbol + "PERP"
    # print(signal_data)
    try:
      await send_signal(signal_data)
    except Exception as e:
      print('except')
      print(e)

async def reader(channel: aioredis.client.PubSub):
    while True:
        try:
            async with async_timeout.timeout(1):
                message = await channel.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    if 'symbol' in message["data"]:
                        await handle_singal(message["data"])
                    else:
                        print(f"(Reader) Message Received: {message}")
                await asyncio.sleep(0.01)
        except Exception as e:
            print(e)



async def main():
    global configures

    try:
        with open("config.json") as f:
            data = f.read()
            configures = json.loads(data)
    except Exception as e:
        print(e)
        exit(0)
    if not configures:
        print("config json file error!")
        exit(0)

    redis = aioredis.from_url("redis://localhost", port=7000, password='bixin123456', decode_responses=True)
    pubsub = redis.pubsub()
    await pubsub.subscribe("signal")

    future = asyncio.create_task(reader(pubsub))
    await future

if __name__ == "__main__":
    asyncio.run(main())

