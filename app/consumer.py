import argparse
import os 
import time
import logging
from confluent_kafka import Consumer

def read_config_props(config_file):
    """Read a standard java properties file."""

    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if line[0] != "#" and len(line) != 0:
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()

    return conf


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(
             description="Surrogate consumer app for testing static group leader")
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    required.add_argument('-f',
                          dest="config_file",
                          help="path to consumer configuration properties file",
                          required=True)
    required.add_argument('-t',
                          dest="topic",
                          help="topic to consumer from",
                          required=True)
    required.add_argument('-p',
                          dest="partition",
                          help="partition to consume",
                          required=True)

    required.add_argument('-l',
                          dest="leader",
                          help="The leader's url for connecting to",
                          required=True)

    args = parser.parse_args()

    return args

def subscribe_to_topic(consumer, topic):
        # Subscribe to topic
    consumer.assign()

    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                total_count += count
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
    
    print(f"Consumed {total_count} messages.")

if __name__ == '__main__':
    args = parse_args()
    props = read_config_props(args.config_file)
    topic = args.topic

    consumer = Consumer({
        'bootstrap.servers': props['bootstrap.servers'],
        'group.id': 'locations-group',
    })

    subscribe_to_topic(consumer, topic)
