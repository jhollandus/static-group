import argparse
import logging
import threading
from typing import Optional

import ujson
from confluent_kafka import Consumer
from confluent_kafka import TopicPartition

from app.static_assignment.assignments import MemberAssignment
from app.static_assignment.group import StaticConfig
from app.static_assignment.group import StaticConsumer
from app.static_assignment.group import StaticMembership
from app.static_assignment.zookeeper import ZkCoordinator

logger = logging.getLogger(__name__)


class ExampleConsumer(StaticConsumer):
    def __init__(self, config: StaticConfig, echo: bool = False):
        self._config = config
        self._consumer: Optional[Consumer] = None
        self._maxMessages = config.more.get('maxMessages', 100)
        self._maxPollTime = float(config.more.get('maxPollTime', 0.2))
        self.totalCount = 0
        self.echo = echo

    def topicPartitionCount(self, topic):
        """Lookups partition count for topic.

        Discovers how many partitions the given topic has. If the topic does not exist then 0 is returned.

        Args:
            topic (str): The name of the topic to find the partition count for.

        Returns:
            int: The partition count or 0 if the topic is invalid
        """
        if self._consumer is None:
            raise ValueError('Invalid state, consumer is not open.')

        clusterMeta = self._consumer.list_topics(topic=topic)
        if topic in clusterMeta.topics:
            return len(clusterMeta.topics[topic].partitions)
        else:
            return 0

    def poll(self):
        """Performs assignment polling and data processing.

        Invoked repeatedly to allow the consumer to perform a single poll and related data processing
        against it's assigned partitions. If commits are being handled manually then the commit action
        should be taken also.

        Any error raised from this method will cause the consumer to leave the group.
        """
        if self._consumer is None:
            raise ValueError('Invalid state, consumer is not open.')

        msgCount = 0
        while msgCount < self._maxMessages:
            msg = self._consumer.poll(self._maxPollTime)
            if msg is None:
                break
            elif msg.error():
                logger.error('Message error: %s', msg.error)
                break
            else:
                if self.echo:
                    print(f'{msg.value()}')
                msgCount += 1
        self.totalCount += msgCount

    def assign(self, assignments: MemberAssignment):
        """Called when partition assignments must be updated.

        This method is invoked whenever assignments have changed for this consumer. This happens after joining the
        group and also whenever assignments have been updated. If for whatever reason the assignment updates fail then
        an error should be raised. This will trigger the consumer to leave the group.

        Args:
            assignments (MemberAssignment): The assignments that should be applied so that the poll() method will return back data from the given topics and partitions.
        """
        if self._consumer is None:
            raise ValueError('Invalid state, consumer is not open.')

        tps = []
        for t, pList in assignments.topics.items():
            tps.extend([TopicPartition(t, p) for p in pList])
        self._consumer.assign(tps)

    def open(self):
        """When invoked the consumer should establish it's initial connection to kafka.

        Invoked prior to doing anything else. The consumer should establish itself and connect to kafka. If an error is encountered then this will cause a
        fast failure and an outside process will need to perform retries.
        """
        if self._consumer is None:
            self._consumer = Consumer(
                {
                    'bootstrap.servers': self._config.kafkaConnect,
                    'group.id': self._config.group,
                    'client.id': self._config.group + '_' + self._config.hostId,
                }
            )

    def close(self):
        """When invoked the consumer should close down and cleanup any resources it has allocated.

        Invoked when the process is being shutdown usually. Gives the consumer a chance to gracefully close itself down including
        unassigning itself from any partitions if it still has active assignments. An error will cause a fast failure.
        """
        if self._consumer is not None:
            self._consumer.close()

    def onJoin(self):
        """Optional method to implement, is invoked whenever the consumer joins the group
        """
        logger.debug('OnJoin called')

    def onLeave(self):
        """Optional method to implement, is invoked whenever the consumer leaves the group
        """
        logger.debug('OnLeave called')


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Example consumer app for testing static group leader')
    parser.add_argument('--example', action='store_true', help='Prints an example JSON configuration to the stdout')
    parser.add_argument('--echo', action='store_true', help='Print out each message consumed to stdout.')
    parser.add_argument('config_file', nargs='?', help='path to consumer configuration JSON file')

    args = parser.parse_args()

    return args


def main():
    args = parse_args()

    if args.example:
        print(
            '''{
    "hostId": "localhost",
    "topics": ["topic_one", "topic_two"],
    "group": "example-group",
    "maxGroupSize": 2,
    "configVersion": 1,
    "zkConnect": "localhost:2181",
    "kafkaConnect": "localhost:9092",
    "more": {
        "maxPollingTime": 0.2,
        "maxMessages": 100
    }
}'''
        )
    else:
        logging.basicConfig(level=logging.DEBUG)
        conf = None
        with open(args.config_file) as f:
            conf = StaticConfig.fromDict(ujson.load(f))

        stCoordinator = ZkCoordinator.fromGroup(conf.zkConnect, conf.group)
        stConsumer = ExampleConsumer(conf, args.echo)
        stMembership = StaticMembership(conf, stConsumer, stCoordinator)

        def membershipThread():
            stMembership.start()

        try:
            stThread = threading.Thread(target=membershipThread)
            stThread.start()
            stThread.join()
        except KeyboardInterrupt:
            logger.error('Aborted by user')
        finally:
            stMembership.stop()
            stThread.join()

        logger.info('Consumed %s messages', stConsumer.totalCount)


if __name__ == '__main__':
    main()
