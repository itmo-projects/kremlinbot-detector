from fastapi import FastAPI
from pydantic import BaseModel
from kafka.config import kafka_config
from kafka import KafkaProducer
import json
import uvicorn


# from somewhere import kafka_config

class Link(BaseModel):
    link: str


app = FastAPI()


@app.post("/group_link")
def create_course(link: Link):
    producer.send(kafka_config.group_update, value=link)


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=kafka_config.bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    uvicorn.run(app, host="0.0.0.0", port=8000)
