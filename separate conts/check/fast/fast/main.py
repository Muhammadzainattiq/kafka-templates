from contextlib import asynccontextmanager
from typing import Union, Optional, Annotated, AsyncGenerator
from sqlmodel import Field, Session, SQLModel, create_engine, select, Sequence
from fastapi import FastAPI, Depends
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
import asyncio


class Order(SQLModel):
    id: int
    price: int
    thing: str


async def consume_messages(topic, bootstrap_servers):
    consumer = AIOKafkaConsumer(
        topics=topic,
        bootstrap_servers=bootstrap_servers,
        group_id="order",
        auto_offset_reset="earliest"
    )

    await consumer.start()
    try:
        async for message in consumer:
            print(f"Received message: {message.value.decode()} on topic {message.topic}")
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    task = asyncio.create_task(consume_messages("topic", "broker:19092"))
    try:
        yield
    finally:
        task.cancel()
        # try:
        #     await task
        # except asyncio.CancelledError:
        #     pass


app = FastAPI(
    lifespan=lifespan,
    title="Hello World API with DB",
    version="0.0.1",
    servers=[
        {
            "url": "http://localhost:8000",  # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        }
    ]
)


@app.get("/")
def read_root():
    return {"Kafka": "Messagings hello"}


@app.post('/create_order')
async def create_order(order: Order):
    producer = AIOKafkaProducer(bootstrap_servers="broker:19092")
    await producer.start()
    orderJSON = json.dumps(order.__dict__).encode('utf-8')
    print(orderJSON)
    try:
        await producer.send_and_wait("changed", orderJSON)
    finally:
        await producer.stop()
    return orderJSON
