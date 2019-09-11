import signal
import argparse
import logging
import threading
from collections import Mapping
from os import environ
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import ujson
from confluent_kafka import Consumer
from confluent_kafka import TopicPartition

from app.static_assignment.assignments import MemberAssignment
from app.static_assignment.group import StaticConfig
from app.static_assignment.group import StaticConsumer
from app.static_assignment.group import StaticMembership
from app.static_assignment.zookeeper import ZkCoordinator

logger = logging.getLogger(__name__)


class ConfigCollector:
    def __init__(self):
        """Simplifies collecting configuration from multiple sources with each overriding the previous.

        Collects configurations from many sources and allows one source to override the other's
        key value pairs. The order that sources are added is important the result of the final configuration
        is dependent on it.

        To use this one first must add the different sources that configuration may come from those include:

         . A dictionary
         . A JSON file
         . Environmental variables

        All sources are then flattenned with each value path being concatenated by a colon `:`
        then they are merged together in order from first to last, last in wins.
        All paths will be made lower case so keys such as 'FooBar' and 'foobar' will collide.
        Also the colon `:` is reserved and will be stripped from the key.
        Below are some examples how paths are transformed and flattened into keys:

            {'MyKey': 'MyKeyVal'} -> {mykey: 'MyKeyVal'}
            {'k_1': 'val1', 'k:2': 'val2', 'k.3': 'val3} -> {'k_1': 'val1', 'k2': 'val2', 'k.3': 'val3'}
            {'a': 'v1', b: {'a': 'v2}} -> {'a': 'v1', 'b:a': 'v2'}

        """

        self._sources = []

    def addDict(self, dictConfig: Dict[str, Any]) -> 'ConfigCollector':
        """Add a dictionary source

        The dictionary source may be hierarchical and any values that are dictionaries themselves will
        be traversed and flattened. All keys must be strings, if a non string key is found then a ValueError will be raised at the time of parsing.

        Args:
            dictConfig (Dict[str,Any]): A dictionary containing configuration
        """

        def _fn():
            return self._parseDict(dictConfig)

        self._sources.append(_fn)
        return self

    def addJson(self, jsonFilePath: str) -> 'ConfigCollector':
        """Add a JSON file source

        The source file must exist, be readable and conform to json standards as understood by python.
        If any of those requirements are not met then a ValueError will be raised at the time of parsing.

        Any hierarchy found in the json file will be flattened.

        Args:
            jsonFilePath (str): The path to a JSON file
        """

        def _fn():
            rs = []
            with open(jsonFilePath) as f:
                rs = self._parseDict(ujson.load(f))
            return rs

        self._sources.append(_fn)
        return self

    def addEnv(self, prefix: str = 'STAS_', replace_: str = '.') -> 'ConfigCollector':
        """Add an environmental variable source

        Given a prefix the process environment will be scanned for any variables that start with the prefix.
        The prefix will then be removed and the remaining variable name will be used as the key.
        Underscores found in the name will be transformed to the value given by the `replace_` parameter,
        double underscores `__` will be considered a colon (sub hierarchy).

        All values are considered to be strings.

        Examples:

            STAS_FOO=bar -> {'foo': 'bar'}
            STAS_MyFoo=bar -> {'myfoo': 'bar'}
            STAS_SUBS__FOO=subsbar -> {'subs:foo': 'subsbar'}
            STAS_FOO_BAR=fubar -> {'foo.bar': 'fubar'}

        Args:
            prefix (str): The prefix to look for when selecting environmental variables. `STAS_` by default
            replace_ (str): The value to use to replace underscores with. Dot `.` by default.
        """

        def _fn():
            return self._parseEnv(prefix, replace_)

        self._sources.append(_fn)
        return self

    def parseFlat(self) -> Dict[str, Any]:
        """Parses configured sources into a flat representation

        Invoke this after adding sources to parse all the sources, flatten them and then merge them.

        May raise any errors associated with the configured sources.

        Returns:
            Dict[str, Any]: A dictionary containing flattened results all hierarcy will be stripped but original types as determined by the source will be preserved.
        """
        rs = {}
        for src in self._sources:
            rs.update(src())

        return rs

    def parse(self) -> Dict[str, Any]:
        """Parses configured sources, maintaining hierarchy"""
        rs = {}
        for src in self._sources:
            for k, v in src():
                pathParts = k.split(':')
                vDict = rs
                for part in pathParts[:-1]:
                    if part in vDict:
                        vDict = vDict[part]
                    else:
                        vDict[part] = {}
                        vDict = vDict[part]
                vDict[pathParts[-1:][0]] = v
        return rs

    def _keyPath(self, path: List[str], key: str) -> str:
        p = list(path)
        p.append(key)
        rs = ':'.join(p)
        return rs

    def _formatKey(self, rawKey: str) -> str:
        return rawKey.replace(':', '').lower()

    def _parseEnv(self, prefix: str, replace_: str) -> List[Tuple[str, Any]]:
        rs = []
        for k, v in environ.items():
            if k.startswith(prefix):
                key = self._formatKey(k[len(prefix) :].replace('_', replace_)).replace('__', ':')
                rs.append((key, v))
        return rs

    def _parseDict(self, aDict: Dict[str, Any], _path: List[str] = []) -> List[Tuple[str, Any]]:

        rs = []
        for k, v in aDict.items():
            key = self._formatKey(k)
            if isinstance(v, Mapping):
                p = list(_path)
                p.append(key)
                rs.extend(self._parseDict(v, p))
            else:
                rs.append((self._keyPath(_path, key), v))
        return rs


class ExampleConsumer(StaticConsumer):
    def __init__(self, config: StaticConfig, echo: bool = False):
        self._config = config
        self._consumer: Optional[Consumer] = None
        self._maxMessages = config.more.get('max.messages', 100)
        self._maxPollTime = float(config.more.get('max.poll.time', 0.2))
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
        for msg in  self._consumer.consume(self._maxMessages, self._maxPollTime):
            if msg is None:
                break
            elif msg.error():
                logger.error('Message error: %s', msg.error)
                break
            else:
                if self.echo:
                    print(f'{msg.value()}')
                self._consumer.store_offsets(msg)
                msgCount += 1

        logger.info('Consumed %s messages', msgCount)
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

        logger.info('Consumer assigning new topic partitions: %s', assignments)
        tps = []
        for t, pList in assignments.topics.items():
            tps.extend([TopicPartition(t, p) for p in pList])
        self._consumer.unassign()
        self._consumer.assign(tps)
        logger.info('Consumer assignment complete: %s', assignments)

    def open(self):
        """When invoked the consumer should establish it's initial connection to kafka.

        Invoked prior to doing anything else. The consumer should establish itself and connect to kafka. If an error is encountered then this will cause a
        fast failure and an outside process will need to perform retries.
        """
        props = {}
        if self._config.more is not None and 'consumer' in self._config.more:
            props.update(self._config.more['consumer'])

        props.update({'bootstrap.servers': self._config.kafkaConnect,
                      'group.id': self._config.group,
                      'client.id': self._config.group + '_' + self._config.hostId,
                    })

        logger.info('Using following configuration for the consumer: %s', props)
        if self._consumer is None:
            self._consumer = Consumer(props)
        logging.debug('Consumer connection open')

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
    parser.add_argument(
        '-c', '--config', dest='config_file', nargs='?', help='path to consumer configuration JSON file'
    )

    args = parser.parse_args()

    return args

def main():
    args = parse_args()

    if args.example:
        print(
            '''{
    "host.id": "localhost",
    "topics": ["topic_one", "topic_two"],
    "group": "example-group",
    "max.group.size": 2,
    "config.version": 1,
    "zk.connect": "localhost:2181",
    "kafka.connect": "localhost:9092",
    "more": {
        "max.polling.time": 0.2,
        "max.messages": 100
    }
}'''
        )
    else:
        logging.basicConfig(level=logging.DEBUG)
        config = ConfigCollector()
        if args.config_file is not None:
            config.addJson(args.config_file)
        config.addEnv()

        parsedConfig = config.parse()
        logger.debug('Using configuration: %s', parsedConfig)
        conf = StaticConfig.fromDict(parsedConfig)

        stCoordinator = ZkCoordinator.fromGroup(conf.zkConnect, conf.group)
        stConsumer = ExampleConsumer(conf, args.echo)
        stMembership = StaticMembership(conf, stConsumer, stCoordinator)

        def sigterm_handler(signal, frame):
            logger.info("SIGTERM received, shutting down")
            stMembership.stop()
        signal.signal(signal.SIGTERM, sigterm_handler)

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
