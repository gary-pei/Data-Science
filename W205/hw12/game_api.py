#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request
app = Flask(__name__)
event_logger = KafkaProducer(bootstrap_servers='kafka:29092')
events_topic = 'events'

def log_to_kafka(topic, event):
    event.update(request.headers)
    event_logger.send(topic, json.dumps(event).encode())

@app.route("/purchase_a_sword")
def purchase_sword():
    purchase_sword_event = {'event_type': 'purchase_sword'}
    sword_params = request.args.to_dict()
    purchase_sword_event.update(sword_params)
    log_to_kafka(events_topic, purchase_sword_event)
    return "\nSword Purchased!\n"

@app.route("/join_guild")
def join_guild():
    joined_guild_event = {'event_type': 'join_guild'}
    log_to_kafka(events_topic, joined_guild_event)
    return "\nGuild joined!\n"
