import logging
import asyncio
import pika

from contextlib import asynccontextmanager

from pika.channel import Channel
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.exceptions import ChannelClosedByClient, AMQPError

from migration_service.settings import settings

logger = logging.getLogger(__name__)


class PikaChannel:
    conn = None

    def __init__(self, channel: Channel):
        self._channel = channel
        self._close_callbacks = []
        self._channel.add_on_close_callback(self._close_callback)

    def _close_callback(self, *args, **kwargs):
        for cbk in self._close_callbacks:
            cbk(*args, **kwargs)

    @staticmethod
    @asynccontextmanager
    async def wait_for_callback(wait_for: str):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def callback(method):
            try:
                if method.method.NAME == wait_for:
                    fut.set_result(wait_for)
                    return
            except Exception as e:
                fut.set_exception(e)
                return
            fut.set_exception(Exception(method.method.NAME))

        yield callback
        await fut

    async def queue_declare(self, queue: str):
        async with self.wait_for_callback('Queue.DeclareOk') as callback:
            self._channel.queue_declare(queue, callback=callback, durable=True)

    async def exchange_declare(self, exchange: str, exchange_type: str):
        async with self.wait_for_callback('Exchange.DeclareOk') as callback:
            self._channel.exchange_declare(exchange, exchange_type, callback=callback, durable=True)

    async def queue_bind(self, queue: str, exchange: str, routing_key: str = None):
        async with self.wait_for_callback('Queue.BindOk') as callback:
            self._channel.queue_bind(queue, exchange, routing_key, callback=callback)

    async def basic_ack(self, delivery_tag: int):
        self._channel.basic_ack(delivery_tag)

    async def basic_reject(self, delivery_tag: int, requeue: bool):
        self._channel.basic_reject(delivery_tag, requeue=requeue)

    async def basic_publish(self, exchange: str, routing_key: str, body: bytes):
        self._channel.basic_publish(exchange, routing_key, body)

    async def consume(self, queue: str) -> bytes:
        loop = asyncio.get_running_loop()
        messages = asyncio.Queue()
        self._channel.basic_consume(
            queue,
            on_message_callback=lambda _channel, method, _props, body: messages.put_nowait((method.delivery_tag, body)),
            auto_ack=False
        )

        fut = loop.create_future()

        def on_close_callback(ch: Channel, reason: AMQPError):
            try:
                raise reason
            except ChannelClosedByClient:
                fut.cancel()
            except AMQPError:
                fut.set_exception(reason)

        self._close_callbacks.append(on_close_callback)
        try:
            while True:
                msg = asyncio.ensure_future(messages.get())
                await asyncio.wait([msg, fut], return_when=asyncio.FIRST_COMPLETED)

                if fut.done():
                    if fut.cancelled():
                        logger.debug('Chanel gracefully closed, exiting loop')
                        break
                    raise fut.exception()

                delivery_tag, body = msg.result()
                yield delivery_tag, body

        except AMQPError as amqp_err:
            logger.error(amqp_err)
        finally:
            self._close_callbacks.remove(on_close_callback)


async def create_connection():
    if PikaChannel.conn:
        return PikaChannel.conn

    loop = asyncio.get_running_loop()

    fut = loop.create_future()
    AsyncioConnection(
        pika.URLParameters(settings.mq_connection_string),
        on_open_callback=lambda c: fut.set_result(c),
        on_open_error_callback=lambda c, exc: fut.set_exception(exc),
        on_close_callback=lambda c, exc: fut.set_exception(exc)
    )
    conn = await fut
    PikaChannel.conn = conn
    return PikaChannel.conn


@asynccontextmanager
async def create_channel() -> PikaChannel:
    loop = asyncio.get_running_loop()
    conn = await create_connection()

    try:
        fut = loop.create_future()
        conn.channel(on_open_callback=lambda ch: fut.set_result(ch))
        channel = await fut

        try:
            yield PikaChannel(channel)
        finally:
            channel.close()

    finally:
        PikaChannel.conn = None
