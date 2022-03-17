
import asyncio
import async_timeout
import aioredis
import aiohttp
import json
from datetime import datetime

configures = {}

index = 0

async def send_signal(signal_data: dict, message: dict) -> (dict, bool):
    print('时间:', datetime.now(), signal_data['side'], signal_data['symbol'])
    headers = {"Content-Type": "application/json; charset=utf-8"}

    # timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession() as session:
        # resp = await session.post(configures['url'], headers=headers, data=json.dumps(signal_data))
        async with session.post(configures['url'], headers=headers, data=json.dumps(signal_data)) as resp:
            text = await resp.json()
            print("resp11:",  datetime.now(), resp.status, text)

        async with session.post("http://localhost:5000/register", headers=headers, data=json.dumps(message)) as resp:
            text = await resp.text()
            print("resp22:",  datetime.now(), resp.status, text)
    print('****')

async def handle_singal(message_str):
    global index
    index+=1
    message = json.loads(message_str)
    symbol = message["symbol"]
    symbol = symbol.replace('/', '')
    message["symbol"] = symbol + "PERP"
    message["index"] = index
    print(message)

    signal_data = configures['data']
    signal_data['side'] = message['action']
    signal_data['symbol'] = symbol + "PERP"
    # print(signal_data)
    
    await send_signal(signal_data, message)


async def reader(channel: aioredis.client.PubSub):
    while True:
        try:
            async with async_timeout.timeout(10):
                message = await channel.get_message(ignore_subscribe_messages=True)
                await asyncio.sleep(0.01)
            if message is not None:
                if 'symbol' in message["data"]:
                    await handle_singal(message["data"])
                else:
                    print(f"(Reader) Message Received: {message}")
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

