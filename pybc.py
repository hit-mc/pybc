from __future__ import annotations

import logging
import os
import threading
import time
from abc import ABC
from queue import Queue
from typing import Iterator, Callable, Union, BinaryIO

import bson
import redis
from bson.codec import Int64
from redis.client import PubSub


class MessageType:
    """
    Type constants of message blocks defined in BungeeCross protocol.
    """

    TEXT = 0
    IMG = 1


def generate_topic():
    """
    Generate a random hex topic string.
    :return: the string.
    """
    return os.urandom(8).hex().lower()


class _HookedPubSub(PubSub):

    def __init__(self, connection_pool, on_reset, **kwargs):
        self.on_reset = on_reset
        super().__init__(connection_pool, **kwargs)

    def close(self):
        super().close()
        self.on_reset()


class _Receiver:

    def __init__(self, pattern: bytes, channel_filter, pool: redis.ConnectionPool,
                 message_handler: Callable[[Message], None]):
        super().__init__()
        if not isinstance(pattern, bytes):
            raise ValueError('pattern must be bytes')
        self.pool = pool
        self.pattern = pattern
        self.channel_filter = channel_filter
        self.logger = logging.getLogger(_Receiver.__name__)
        self.thread = None
        self.__handler = message_handler

    def start(self):
        con = redis.Redis(connection_pool=self.pool)
        # replace self.con.pubsub() with HookedPubSub,
        # hack Redis.pubsub() method to inject our pubsub.on_close hook
        pubsub = _HookedPubSub(con.connection_pool, self.__on_close)
        pubsub.psubscribe(**{self.pattern.decode('utf-8'): self.__on_message})
        self.thread = pubsub.run_in_thread()

    def __on_message(self, msg):
        self.logger.info('Received data: %s' % msg)
        pattern = msg.get('pattern')
        channel = msg.get('channel')
        if pattern != self.pattern or not self.channel_filter(channel):
            return  # filtered
        data = msg.get('data')
        if not isinstance(data, bytes):
            self.logger.info(f'Discard non-bytes inbound message: {msg}')
            return
        # try to decode as BSON
        decoded = Message.deserialize(data)
        self.__handler(decoded)

    def __on_close(self):
        self.logger.info("Receiver is quit.")

    def stop(self):
        if thread := self.thread:
            thread.stop()


class _Sender(threading.Thread):

    def __init__(self, channel, pool: redis.ConnectionPool):
        super().__init__()
        self.pool = pool
        self.channel = channel
        self.queue = Queue()
        self.logger = logging.getLogger(_Sender.__name__)

    def run(self):
        with redis.Redis(connection_pool=self.pool) as con:
            while True:
                msg = self.queue.get(block=True)
                if msg is None:
                    break
                if not isinstance(msg, Message):
                    self.logger.warning(f'Object taken from send queue'
                                        f'is not a Message instance: {msg}')
                    continue
                count = con.publish(self.channel, msg.serialize())
                self.logger.info(f'Sent to {count} peers.')
        self.logger.info('Sender is quit.')

    def stop(self):
        self.queue.put(None)

    def send_message(self, message: Message):
        if not isinstance(message, Message):
            raise ValueError('message must be a Message instance')
        self.queue.put(message)


class BungeeCross:
    """
    The BungeeCross protocol peer.
    It connects to a specific Redis server,
    send and receive messages to/from it,
    using BungeeCross protocol.
    """

    def __init__(self, **kwargs):
        self.connection_config = dict(kwargs['connection_config'])
        self.topic_config = dict(kwargs['topic_config'])
        self.channel_name = f'{self.topic_config["prefix"]}{self.topic_config["topic"]}'
        self.redis_pool = redis.ConnectionPool(host=self.connection_config['host'],
                                               port=self.connection_config['port'],
                                               password=self.connection_config['pass'])
        self.subscribe_pattern = BungeeCross.__get_subscribe_pattern(
            self.topic_config['prefix']).encode('utf-8')
        self.sender = _Sender(self.channel_name, self.redis_pool)
        self.receiver = _Receiver(self.subscribe_pattern,
                                  self.__channel_filter, self.redis_pool,
                                  kwargs.get('message_handler') or self.__default_inbound_message_handler)
        self.logger = logging.getLogger(BungeeCross.__name__)

    def start(self):
        self.logger.info(f'Starting BungeeCross with pattern `{self.subscribe_pattern}`')
        self.sender.start()
        self.receiver.start()

    def stop(self):
        self.sender.stop()
        self.receiver.stop()

    def send_message(self, message: Message):
        if not isinstance(message, Message):
            raise ValueError('message is not a Message instance')
        self.sender.send_message(message)

    def __default_inbound_message_handler(self, message: Message):
        self.logger.info(f'Inbound Message: {message}')

    @staticmethod
    def __get_subscribe_pattern(prefix):
        return f'{prefix}*'

    def __channel_filter(self, channel_name):
        return channel_name != self.channel_name


class BungeeCrossBuilder:
    """
    A builder creates BungeeCross instance.
    Easier for users to fill in setup parameters.
    """

    def __init__(self):
        self.connection_config = dict()
        self.topic_config = dict()

    def connection(self, host, port=6379, password=None) -> BungeeCrossBuilder:
        int_port = int(port)
        if int_port <= 0 or int_port >= 65536:
            raise ValueError('invalid port: %d' % int_port)
        self.connection_config = {
            'host': host,
            'port': int_port,
            'pass': password or '',
        }
        return self

    def topic(self, topic, prefix='bc_') -> BungeeCrossBuilder:
        """
        Topic is where BungeeCross endpoints talks to.
        Each topic is sent to individual Redis channels.
        """
        self.topic_config = {
            'topic': topic,
            'prefix': prefix
        }
        return self

    def build(self) -> BungeeCross:
        return BungeeCross(connection_config=self.connection_config,
                           topic_config=self.topic_config)


class Message(ABC):

    """
    An **immutable** class containing send time, sender name, sender endpoint and message blocks.
    """

    @property
    def time(self) -> int:
        raise NotImplementedError()

    @property
    def endpoint(self) -> str:
        raise NotImplementedError()

    @property
    def sender(self) -> str:
        raise NotImplementedError()

    @property
    def message_blocks_tuples(self) -> Iterator[tuple[int, bytes]]:
        raise NotImplementedError()

    @property
    def message_blocks(self) -> Iterator[MessageBlock]:
        """
        Default implementation which construct objects from raw tuples.
        The result will be cached for next calls.
        """
        prop = '__cached_message_blocks'
        if not hasattr(self, prop):
            blocks = [MessageBlock.from_tuple(tpl) for tpl in self.message_blocks_tuples]
            setattr(self, prop, blocks)
        return getattr(self, prop, None) or []

    @property
    def textual_message(self) -> str:
        return ''.join([str(msg) for msg in self.message_blocks])

    def serialize(self) -> bytes:
        return bson.dumps({
            'endpoint': self.endpoint,
            'sender': self.sender,
            'msg': tuple(self.message_blocks_tuples),
            'time': Int64(self.time)
        })

    @staticmethod
    def deserialize(data: bytes):
        m = bson.loads(data)
        return SimpleMessage(endpoint=m['endpoint'], sender=m['sender'],
                             blocks=[tuple(x) for x in m['msg']], time=m['time'])

    def __str__(self) -> str:
        return f'Message(time={time.ctime(self.time)}, sender={self.sender}, ' \
               f'endpoint={self.endpoint}, message={self.textual_message})'


class MessageBlock:
    """
    MessageBlock interface defines an abstraction of message blocks in BungeeCross protocol.
    A message is constituted by message blocks. A message block may be a UTF-8 string,
    or an image file of some certain types (JPEG, PNG, etc.), or something else to be added
    in the future versions of BungeeCross protocol.
    """

    @property
    def type(self) -> int:
        raise NotImplementedError()

    @property
    def raw_data(self) -> bytes:
        raise NotImplementedError()

    @staticmethod
    def from_tuple(tpl: tuple[int, bytes]):
        typ, data = tpl
        if typ == MessageType.TEXT:
            return TextualMessageBlock(data)
        elif typ == MessageType.IMG:
            return ImageMessageBlock(data)
        else:
            return UnknownTypeMessageBlock(typ, data)


@DeprecationWarning
class SimpleMessageBlock(MessageBlock):
    """
    This class is too simple. It is not encouraged to use this.
    Use other implementations of MessageBlock with certain types instead.
    """

    def __init__(self, tpl: tuple[int, bytes]):
        self._type, self._raw_data = tpl

    @property
    def type(self) -> int:
        return self._type

    @property
    def raw_data(self) -> bytes:
        return self._raw_data

    def __str__(self) -> str:
        return ''.join(['(type=', str(self.type), ', bytes=', str(len(self.raw_data)),
                        ', data=', str(self._raw_data), ')'])


class TextualMessageBlock(MessageBlock):
    """
    A UTF-8 text message block.
    """

    def __init__(self, text: Union[bytes, str]):
        if isinstance(text, bytes):
            self._text_bytes = text
            self._text_str = text.decode('utf-8')
        else:
            text = str(text)
            self._text_bytes = text.encode('utf-8')
            self._text_str = text

    @property
    def type(self) -> int:
        return MessageType.TEXT

    @property
    def raw_data(self) -> bytes:
        return self._text_bytes

    def __str__(self) -> str:
        return self._text_str


class ImageMessageBlock(MessageBlock):
    """
    An image message block, containing nothing but an image of a certain format.
    """

    def __init__(self, data: Union[bytes, BinaryIO]):
        if isinstance(data, BinaryIO):
            data = data.read()
        if not isinstance(data, bytes):
            raise ValueError('image data must be bytes or IO which produces bytes')
        self._data = data

    @property
    def type(self) -> int:
        return MessageType.IMG

    @property
    def raw_data(self) -> bytes:
        return self._data


class UnknownTypeMessageBlock(MessageBlock):
    """
    Representing a message of the types which this implementation cannot recognize.
    """

    def __init__(self, typ: int, data: bytes):
        if not isinstance(typ, int):
            raise ValueError('type is not an int')
        if not isinstance(data, bytes):
            raise ValueError('data is not a bytes')
        self._type = typ
        self._data = data

    @property
    def type(self) -> int:
        return self._type

    @property
    def raw_data(self) -> bytes:
        return self._data


class SimpleMessage(Message):
    """
    An immutable message which has been initialized in its constructor.
    """

    def __init__(self, sender: str, endpoint: str,
                 time: int, blocks: Iterator[tuple[int, bytes]]):
        self._sender = sender
        self._endpoint = endpoint
        self._time = time
        self._blocks = tuple(blocks)

    @property
    def time(self) -> int:
        return self._time

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @property
    def sender(self) -> str:
        return self._sender

    @property
    def message_blocks_tuples(self) -> Iterator[tuple[int, bytes]]:
        return self._blocks
