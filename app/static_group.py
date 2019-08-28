import argparse
import os 
import time
import logging
from typing import Tuple, Dict
from kazoo_sasl.client import KazooClient
from confluent_kafka import Consumer

logging.basicConfig()

class TopicPartitions(object):

    def __init__(self, topic: str, partitions: Tuple[int]):
        self.topic = topic
        self.partitions = partitions


class StaticAssignment(object):

    def __init__(self, 
                 version: int,
                 configVersion: int,
                 assignments: Dict[int, TopicPartitions],
                 members=None):
                    
        self.version = version
        self.configVersion = configVersion
        self.assignments = assignments
        self.members = members


class StaticConsumer(object):
    """StaticMembership consumer interface.

    Implement this class and all methods that are not prefixed with 'on' (those methods are optional)
    to include your consumer in the StaticMembership strategy for managing consumer group partition assignments.

    The required methods by default raise NotImplementedError if invoked.
    """

    def topicPartitionCount(self, topic):
        """Lookups partition count for topic.

        Discovers how many partitions the given topic has. If the topic does not exist then 0 is returned.

        Args:
            topic (str): The name of the topic to find the partition count for.

        Returns:
            int: The partition count or 0 if the topic is invalid
        """
        raise NotImplementedError

    def poll(self):
        """Performs assignment polling and data processing.

        Invoked repeatedly to allow the consumer to perform a single poll and related data processing
        against it's assigned partitions. If commits are being handled manually then the commit action
        should be taken also.

        Any error raised from this method will cause the consumer to leave the group.
        """
        raise NotImplementedError

    def assign(self, assignments):
        """Called when partition assignments must be updated.

        This method is invoked whenever assignments have changed for this consumer. This happens after joining the
        group and also whenever assignments have been updated. If for whatever reason the assignment updates fail then
        an error should be raised. This will trigger the consumer to leave the group.

        Args:
            assignments (Assignment): List of topic partitions that the consumer should be consuming in the poll() method
        """
        raise NotImplementedError

    def open(self):
        """When invoked the consumer should establish it's initial connection to kafka.

        Invoked prior to doing anything else. The consumer should establish itself and connect to kafka. If an error is encountered then this will cause a
        fast failure and an outside process will need to perform retries.
        """
        raise NotImplementedError

    def close(self):
        """When invoked the consumer should close down and cleanup any resources it has allocated.

        Invoked when the process is being shutdown usually. Gives the consumer a chance to gracefully close itself down including
        unassigning itself from any partitions if it still has active assignments. An error will cause a fast failure.
        """
        raise NotImplementedError

    def onJoin(self):
        """Optional method to implement, is invoked whenever the consumer joins the group
        """
        pass

    def onLeave(self):
        """Optional method to implement, is invoked whenever the consumer leaves the group
        """
        pass


class StaticConfig(object):
    """Configuration for static membership.

    Attributes:
        hostId=None (str): A unique identifier for the host instance of this process
        topic=None (str): The target topic for the consumers to consume from
        maxGroupSize=None (int): The maximum membership count expected for the static group
        configVersion=None (int): The version of the configuration, used to prevent reverting of assignments
    """
    def __init__(self, 
        hostId=None,
        topic=None,
        maxGroupSize=None,
        configVersion=None):

        self.hostId = hostId
        self.topic = topic
        self.maxGroupSize = maxGroupSize
        self.configVersion = configVersion


class StaticCoordinator(object):
    def updateAssignments(self, config: StaticConfig, memberId: int, newAssignments: StaticAssignment):
        raise NotImplementedError

    def leave(self, config: StaticConfig, memberId: int, assignment: StaticAssignment):
        raise NotImplementedError

    def join(self, config: StaticConfig, memberId: int, assignment: StaticAssignment) -> int: 
        raise NotImplementedError
    
    def assignments(self, config: StaticConfig, memberId: int, assignment: StaticAssignment) -> StaticAssignment:
        raise NotImplementedError

    def heartbeat(self, config: StaticConfig, memberId: int, assignment: StaticAssignmant):
        """
        A heartbeat must be performed as part of the membership contract. 
        """
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError














class StaticMembership(object):
    """
    Represents membership to a statically assigned kafka consumer group.
    Designed as a simple state machine a client using this interface must
    adhere to the following contract:

    1. new members must first perform a 'Join'
    2. Once an assignment is given the consumer must begin consuming 
       from the assigned topic partitions
    3. Once consuming begins the consumer must, as part of their poll loop,
       call `heartbeat()`, if they fail to call heartbeat then they will 
       eventually be unassigned
    4. Whenever a new assignment is given the consumer must update accordingly
    5. When a consumer is going to gracefully leave they should stop consuming
       and then call `leave()`
    
    State diagram below:

    new -> [start()] -> [coord.join()] -> joined -> [coord.heartbeat()] -> joined
    [coord.assignmentReceived] -> [consumer.assign()]

    assigned -> [consumer.poll()] -> [coord.heartbeat()] -> [coord.assignment()] -> [consumer.poll()] 
                                          -> [leave()] -> [consumer.onLeave()] -> new
                              -> [stop()] -> [consumer.onLeave()] -> new

    Calling a method when not in the correct state will elicit an error, e.g. calling 
    `leave()` when in the 'new' state.
    """

    def __init__(self, 
                 config: StaticConfig,
                 staticConsumer: StaticConsumer,
                 coordinator: StaticCoordinator):

        self.STATE_STOPPED = 'stopped'
        self.STATE_STOPPING = 'stopping'
        self.STATE_NEW = 'new'
        self.STATE_JOINED = 'joined'
        self.STATE_ASSIGNED = 'assigned'

        self._state = self.STATE_NEW
        self._assignment = None
        self._memberId = None
        self._partitionCount = None
        self._conf = config
        self._cons = staticConsumer
        self._coord = coordinator

    def _calculatAssignments(self, topic, pCount, maxSize):
        baseNum = int(pCount / maxSize)
        remainder = pCount % maxSize 

        assignments = []
        lowAvailPart = 0
        for id in range(maxSize):
            increment = baseNum
            if remainder > 0:
                increment = baseNum + 1
                remainder = remainder - 1

            lastPart = lowAvailPart + increment
            assignments[id] = TopicPartitions(topic, tuple(range(lowAvailPart, lastPart)))
            lowAvailPart = lastPart

        return assignments


    def start(self):

        #TODO: catch errors
        self._cons.open()
        if self._partitionCount is None:
            self._partitionCount = self._cons.topicPartitionCount(self._conf.topic)
        
        assignments = self._coord.assignments(self._conf, self._memberId, self._assignment)
        if self._conf.configVersion > assignments.configVersion:
            newAssignments = self._calculatAssignments(self._conf.topic,
                                                       self._conf.topic,
                                                       self._partitionCount,
                                                       self._conf.maxGroupSize)
            self._coord.updateAssignments(self._conf,
                                          self._memberId,
                                          StaticAssignment(assignments.version + 1,
                                                           self._conf.configVersion,
                                                           newAssignments))

        while self._state != self.STATE_STOPPING:

            if  self._state == self.STATE_NEW:
                self._memberId = self._coord.join(self._conf, self._memberId, self._assignment)

                self._state = self.STATE_JOINED
                self._cons.onJoin()

            newAssignment = self._coord.assignments(self._conf, self._memberId, self._assignment)

            if newAssignment is not None:
                self._assignment = newAssignment
                self._cons.assign(self._assignment)

                if self._state == self.STATE_JOINED:
                    self._state = self.STATE_ASSIGNED
            else:
                self._state == self.STATE_JOINED

            if self._state == self.STATE_ASSIGNED:
                self._cons.poll()

            #if heartbeat fails then transition back to new
            try:
                self._coord.heartbeat(self._conf, self._memberId, self.assignment)
            except:
                self._state = self.STATE_NEW

        # Stopping
        self._coord.leave(self._conf, self._memberId, self._assignment)
        self._cons.onLeave()
        self._cons.close()
        self._state = self.STATE_STOPPED


    def stop(self):
        self._state = self.STATE_STOPPING
        while self._state != self.STATE_STOPPED:
            time.sleep(1)

    def state(self):
        return self._state


class ZkCoordinator(StaticCoordinator):

    def __init__(self, zkConnect, memberPath, assignmentsPath):
        self.zkConnect = zkConnect
        self.memberPath = memberPath
        self.assignmentsPath = assignmentsPath

        self.zk = KazooClient(hosts=zkConnect)
        self.zk.add_listener(self._zkListener())
        self.zk.start()
        self.zk.ensure_path(self.memberPath)
        self.zk.ensure_path(self.assignmentsPath)

        self.memberId = None

        data, stat = self.zk.get(self.assignmentsPath)
        self.currentAssignment = data.decode('utf-8')

        @self.zk.DataWatch(self.assignmentsPath)
        def watchAssignments(data, stat, event):
            self.currentAssignment = data.decode('uft-8')


    def _zkListener(self):
        def listener(state):
            if state == KazooState.LOST:
                self.memberId = None
        return listener

    def updateAssignments(self, config: StaticConfig, memberId: int, newAssignments: StaticAssignment):
        raise NotImplementedError

    def leave(self, config: StaticConfig, memberId: int, assignment: StaticAssignment):
        raise NotImplementedError

    def join(self, config: StaticConfig, memberId: int, assignment: StaticAssignment) -> int: 

    
    def assignments(self, config: StaticConfig, memberId: int, assignment: StaticAssignment) -> StaticAssignment:
        raise NotImplementedError

    def heartbeat(self, config: StaticConfig, memberId: int, assignment: StaticAssignmant):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError