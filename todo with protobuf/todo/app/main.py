# main.py
from contextlib import asynccontextmanager
from typing import Union, Optional, Annotated, AsyncGenerator
from app import settings
from sqlmodel import Field, Session, SQLModel, create_engine, select
from fastapi import FastAPI, Depends, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
from app import todo_pb2

class Todo(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    content: str = Field(index=True)


# only needed for psycopg 3 - replace postgresql
# with postgresql+psycopg in settings.DATABASE_URL
connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)


# recycle connections after 5 minutes
# to correspond with the compute scale down
engine = create_engine(
    connection_string, connect_args={}, pool_recycle=300
)


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


# The first part of the function, before the yield, will
# be executed before the application starts.
async def consume_messages(topic, bootstrap_servers):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="my-group",
        auto_offset_reset='earliest'
    )
    await consumer.start()

    try:
        # Continuously listen for messages.
        async for message in consumer:
            new_todo = todo_pb2.Todo()
            print("Consumer Deserialized Message>>>>", message.value)
            new_todo.ParseFromString(message.value)
            print("Consumer Serialized Message>>>> ", new_todo)

    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    create_db_and_tables()
    asyncio.create_task(consume_messages("topic", "broker:19092"))
    yield


app = FastAPI(
    lifespan=lifespan, 
    title="Hello World API with DB", 
    version="0.0.1",
    servers=[
        {
            "url": "http://localhost:8000",
            "description": "Development Server"
        }
    ]
)


async def get_session() -> AsyncGenerator[Session, None]:
    with Session(engine) as session:
        yield session


async def get_kafka_producer() -> AsyncGenerator[AIOKafkaProducer, None]:
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/todos/", response_model=Todo)
async def create_todo(
    todo: Todo, 
    session: Annotated[Session, Depends(get_session)], 
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
) -> Todo:
    
    protobuf_todo = todo_pb2.Todo()
    protobuf_todo.id = todo.id
    protobuf_todo.content = todo.content
    print(f"Producer Todo Protobuf: {protobuf_todo}")
    serialized = protobuf_todo.SerializeToString()
    print("Producer Serialized", serialized)
    await producer.send_and_wait("topic", serialized)
    # session.add(todo)
    # session.commit()
    # session.refresh(todo)
    return todo


@app.get("/todos/", response_model=list[Todo])
def read_todos(session: Annotated[Session, Depends(get_session)]):
    todos = session.exec(select(Todo)).all()
    return todos
