"""
This is a simple demo for Python BungeeCross Protocol Implementation.
Note: before running this script, you have to make your Redis server configured
    in file `config.py`:
```
REDIS = {
  "host": "...",
  "port": 6379,
  "password": "..." // if no password, remove this line
}

TOPIC = {
  "topic": "pybc-demo",
  "prefix": "bungeecross."
}
```
"""

import logging

import pybc
import config
import time

LOG_FORMAT = '[%(levelname)s][%(asctime)-15s][%(filename)s][%(lineno)d] %(message)s'
logging.basicConfig(format=LOG_FORMAT, level='INFO')

client = None
try:
    bungee = pybc.BungeeCrossBuilder().connection(
        host=config.REDIS['host'],
        port=config.REDIS['port'],
        password=config.REDIS.get('password')
    ).topic(topic=config.TOPIC['topic'],
            prefix=config.TOPIC['prefix'])

    client = bungee.build()
    client.start()
    # client.send_message(pybc.SimpleMessage(
    #     sender='trueKeuin', endpoint='pybc', time=int(time.time()), blocks=[(0, b'Hello, world!')]
    # ))
    time.sleep(1000000)
finally:
    if client:
        client.stop()
