import logging
from time import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import ujson

logging.basicConfig()
logger = logging.getLogger('static_group')

TopicAssignment = Dict[str, List[int]]
Topics = Dict[str, int]
MemberId = int


class MemberAssignment:
    @staticmethod
    def fromPrimitive(pRep):
        if 'memberId' in pRep and 'topics' in pRep and isinstance(pRep['topics'], Dict):

            return MemberAssignment(int(pRep['memberId']), pRep['topics'])

        return None

    def __init__(self, memberId: MemberId, topics: TopicAssignment = {}):
        self.memberId = memberId
        self.topics = topics

    def __str__(self):
        return f'memberId: {self.memberId}, topics: {self.topics}'

    def __repr__(self):
        return self.__str__()

    def __iter__(self):
        index = sorted(self.topics.keys())
        for t in index:
            pIndex = sorted(self.topics[t])
            for p in pIndex:
                yield (t, p)

    def _primitive(self):
        return {'memberId': self.memberId, 'topics': self.topics}

    def totalAssignments(self):
        acc = 0
        for t, p in self.topics.items():
            acc += len(p)
        return acc

    def assign(self, topic: str, partition: int):
        """
        Assign the given topic and partition number to this member.

        Args:
            topic (str): Name of the topic to be assigned
            partition (int): The topic partition number to be assigned
        """
        if topic not in self.topics:
            self.topics[topic] = []

        self.topics[topic].append(partition)


class AssignmentVersion:
    """
    Indicates the version of the assignments associated to a static membership
    group.

    Attributes
        version (int): The version of the assignment, this is a monotonically incremementing integer that changes whenever the Assignments change.
        configVersion (int): This version prevents older process configurations from reverting newer ones. It should always increase over time.
        group (str): The name of the consumer group, allows assignments to be isolated based on group.
    """

    def __init__(self, version: int, configVersion: int, group: str):
        self.version = version
        self.configVersion = configVersion
        self.group = group


class Assignments:
    @staticmethod
    def fromJson(jsonData: str) -> Optional['Assignments']:
        """
        Deserializes JSON as produced by the asJson() method into an Assignments object.

        Args:
            jsonData (str): The serialized Assignments JSON

        return (Assignment): The deserialized Assignment or None if the jsonData is invalid
        """
        assignments = None
        try:
            parsed = ujson.loads(jsonData)
            topics = parsed['topics']
            maxMembers = int(parsed['maxMembers'])
            group = parsed['group']
            configVersion = int(parsed['configVersion'])
            version = int(parsed['version'])
            primitiveMemberAssignments = parsed['memberAssignments']

            memberAssignments = [
                ma
                for ma in [MemberAssignment.fromPrimitive(pma) for pma in primitiveMemberAssignments]
                if ma is not None
            ]

            if isinstance(topics, Dict):
                assignments = Assignments(
                    group, maxMembers, topics, configVersion, version, memberAssignments, len(memberAssignments) < 1
                )
        except Exception:
            logger.exception("Failed to parse Assignments from JSON. json: '%s'", jsonData)

        return assignments

    def __init__(
        self,
        group: str = 'NO_GROUP',
        maxMembers: int = 0,
        topics: Topics = {},
        configVersion: int = 0,
        version: int = 0,
        memberAssignments: List[MemberAssignment] = [],
        doReassign: bool = True,
    ):
        # all distributed topics and partition counts
        self.group = group
        self.topics = topics
        self.maxMembers = maxMembers
        self.version = version
        self.configVersion = configVersion

        # member id to assignments
        self.memberAssignments = memberAssignments
        if doReassign:
            self._reassign()

    def __str__(self):
        return f'{self.memberAssignments}'

    def __repr__(self):
        return self.__str__()

    def _reassign(self):
        calculator = AssignmentCalculator(self.maxMembers, self.topics)
        self.memberAssignments = calculator.generateAssignments(self)

    def asJson(self) -> str:
        """
        Serializes this object into JSON suitable for deserialization using
        the static method fromJson().

        returns (str): The JSON
        """
        j: Dict[str, Any] = {}
        j['group'] = self.group
        j['topics'] = self.topics
        j['maxMembers'] = self.maxMembers
        j['version'] = self.version
        j['configVersion'] = self.configVersion
        j['memberAssignments'] = [ma._primitive() for ma in self.memberAssignments]

        return ujson.dumps(j)

    def assignmentVersion(self) -> AssignmentVersion:
        return AssignmentVersion(self.version, self.configVersion, self.group)

    def changeMaxMembers(self, maxMembers: int) -> bool:
        """
        Updates the maximum number of members that may belong to this group. If this
        update causes a change to the assignments then True will be returned.

        Args:
            maxMembers (int): The new maximum group size, must be greater than 0

        returns (bool): True if the change triggered a redistribution of assignments
        """
        return self._changeMaxMembers(maxMembers)

    def _changeMaxMembers(self, maxMembers: int, doReassign: bool = True) -> bool:
        if not maxMembers > 0:
            raise ValueError(
                f"Invalid maxMembers argument, expected to be greater than 0, but received: '{maxMembers}''"
            )

        changed = self.maxMembers != maxMembers
        if changed:
            self.maxMembers = maxMembers

            if doReassign:
                self._reassign()
        return changed

    def changeTopicPartitions(self, topics: Topics):
        """
        Updates the topic partitions that should be assigned across the members. If
        this change causes an assignment redistribution then True is returned.

        Args:
            topics (Topics): updated dictionary of topics with partition counts

        Returns (bool): True if a change in assignments occurred
        """
        return self._changeTopicPartitions(topics)

    def _changeTopicPartitions(self, topics: Topics, doReassign: bool = True):
        changed = False
        for t, p in topics.items():
            if t not in self.topics or self.topics[t] != p:
                changed = True
                break

        if changed:
            self.topics = topics

            if doReassign:
                self._reassign()

        return changed

    def getMemberAssignment(self, memberId: MemberId):
        """
        Fetches the assignments for the given member ID.

        Args:
            memberId (int): The member ID to find the assignments for

        Returns (MemberAssignment): The assignments for the member ID or None if the member ID is unknown or unassigned
        """
        if len(self.memberAssignments) > memberId:
            return self.memberAssignments[memberId]
        else:
            return None


class AssignmentCalculator:
    """
    Calculate group assigments based on the given criteria.

    Args:
        maxSize (int): The maximum number of assignments to calculate
        topics (Topics): A dictionary of topic names to partition counts that need assigning
    """

    def __init__(self, maxSize: int, topics: Topics):
        self.maxSize = maxSize
        self.topics = topics

    def _partitionsByIndex(self):
        """
        This generator will return (topic, partition number) tuples in a well defined order that
        is breadth first. For example it will return all 0 partitions of each topic first, then all 1 partitions
        etc. until all topic partitions are doled out.
        """

        topics = {}
        for t, p in self.topics.items():
            topics[t] = {'count': p, 'pos': 0}

        sortedTopics = sorted(topics.keys())

        while len(topics) != 0:
            for t in sortedTopics:
                if t in topics:
                    if topics[t]['pos'] < topics[t]['count']:
                        yield (t, topics[t]['pos'])
                        topics[t]['pos'] += 1
                    else:
                        del topics[t]

    def _totalPartitions(self):
        acc = 0
        for t, p in self.topics.items():
            acc += p
        return acc

    def generateAssignments(self, prevAssignments: Assignments = None):
        """
        Generates a new list of assignments for all members based on the state of the calculator. Can take into
        consideration the max group size, topics to distribute and previous assignments.

        Args:
            prevAssignments (Assignments): Can be used to help determine efficient redistribution of assignments

        Returns (List[MemberAssignment]): A list of all member assignments
        """

        if self.maxSize is None or self.maxSize == 0 or self.topics is None or len(self.topics) == 0:
            return []

        tpSize = self._totalPartitions()
        tpIter = self._partitionsByIndex()
        members = [MemberAssignment(i) for i in range(self.maxSize)]

        if self.maxSize >= tpSize:
            perMbrPartitions = 1
            remainders = 0
        else:
            perMbrPartitions = int(tpSize / self.maxSize)
            remainders = tpSize % self.maxSize

        logger.info('perMbrCount: %s, remainders: %s', perMbrPartitions, remainders)
        for mbr in members:
            pCount = perMbrPartitions
            if remainders > 0:
                pCount += 1
                remainders -= 1

            assignCount = 0
            for t, p in tpIter:
                mbr.assign(t, p)
                assignCount += 1
                if assignCount >= pCount:
                    break

        return members


class StaticConsumer:
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

    def assign(self, assignments: MemberAssignment):
        """Called when partition assignments must be updated.

        This method is invoked whenever assignments have changed for this consumer. This happens after joining the
        group and also whenever assignments have been updated. If for whatever reason the assignment updates fail then
        an error should be raised. This will trigger the consumer to leave the group.

        Args:
            assignments (MemberAssignment): The assignments that should be applied so that the poll() method will return back data from the given topics and partitions.
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


class StaticConfig:
    """Configuration for static membership.

    Attributes:
        hostId=None (str): A unique identifier for the host instance of this process
        topics=None (List[str]): The target topics for the consumers to consume from
        group=None (str): A name for the consumer group that can be used to isolate assignments
        maxGroupSize=None (int): The maximum membership count expected for the static group
        configVersion=None (int): The version of the configuration, used to prevent reverting of assignments
        maxPollInterval=5000 (int): The maximum number of milliseconds between heartbeats before a member is considered unhealthy
        zkConnect=None (str): A standard connection string for zookeeper, host:port,host2:port,host3:port
        kafkaConnect=None (str): Standard kafka bootstrap servers string, [proto://]host:port,[proto://]host2:port,[proto://]host3:port
    """

    def __init__(
        self,
        hostId=None,
        topics: List[str] = None,
        group: str = None,
        maxGroupSize=None,
        configVersion=None,
        maxPollInterval=5000,
        zkConnect=None,
        kafkaConnect=None,
    ):

        self.hostId = hostId
        self.topics = topics
        self.group = group
        self.maxGroupSize = maxGroupSize
        self.configVersion = configVersion
        self.maxPollInterval = maxPollInterval
        self.zkConnect = zkConnect
        self.kafkaConnect = kafkaConnect


class StaticMemberMeta:
    """
    Information about a static member that is used for coordination.
    """

    def __init__(self, hostId: str, memberId: MemberId = None, assignmentVersion: AssignmentVersion = None):
        self.hostId = hostId
        self.memberId = memberId
        self.assignmentVersion = assignmentVersion


class StaticCoordinator:
    def __init__(self, cfg: StaticConfig):
        self.cfg = cfg

    def updateAssignments(self, meta: StaticMemberMeta, newAssignments: Assignments):
        """
        Update the current assignments with the new assignment.
        An error should be raised if the update did not succeed.
        """
        raise NotImplementedError

    def leave(self, meta: StaticMemberMeta):
        """
        Leave the group and give up the current memberId and assignments.

        Calling this method when the coordinator is not part of the group or has already left should
        not raise an exception.
        """
        raise NotImplementedError

    def join(self, meta: StaticMemberMeta) -> int:
        """
        Join the consumer group. If memberId is specified then an attempt will be made to acquire that
        ID otherwise the next available one will be taken.

        This method should be idempotent meaning that if the given member ID is already assigned to it then nothing
        should change and no internal join activity should occur.
        """
        raise NotImplementedError

    def assignments(self, meta: StaticMemberMeta) -> Assignments:
        """
        Fetches the most current version of the assignments. In the case where the most current
        version is not accessible then None should be returned.

        returns (Assignments): The current assignments or None if they are not accessible for any reason
        """
        raise NotImplementedError

    def heartbeat(self, meta: StaticMemberMeta) -> int:
        """
        A heartbeat must be performed as part of the membership contract.

        return (int): The memberId currently assigned to the member
        """
        raise NotImplementedError

    def start(self):
        """
        Start the coordinator. One this call it should make any resource allocations,
        network connections and state syncing required prior to making any of the other
        calls.

        This should not throw an exception if called after already being started.
        """
        raise NotImplementedError

    def stop(self):
        """
        Stop this coordinator and have it cleanup any related resource allocations.
        Calling this on an already stopped or unstarted coordinator should not throw an
        exception.
        """
        raise NotImplementedError


class StaticMembership:
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

    def __init__(self, config: StaticConfig, staticConsumer: StaticConsumer, coordinator: StaticCoordinator):

        self.STATE_STOPPED = 'stopped'
        self.STATE_STOPPING = 'stopping'
        self.STATE_NEW = 'new'
        self.STATE_JOINED = 'joined'
        self.STATE_LEFT = 'left'
        self.STATE_ASSIGNED = 'assigned'
        self.STATE_CONSUMING = 'consuming'

        self._errorCount = 0
        self._state = self.STATE_NEW
        self._assignments: Optional[Assignments] = None
        self._memberId: Optional[MemberId] = None
        self._memberAssignment: Optional[MemberAssignment] = None
        self._conf = config
        self._cons = staticConsumer
        self._coord = coordinator

    def _meta(self):
        aVersion = None
        if self._assignments is not None:
            aVersion = self._assignments.assignmentVersion()

        return StaticMemberMeta(self._hostId, self._memberId, aVersion)

    def _fetchAllTopicMetadata(self, topicList: List[str]) -> Topics:
        topics = dict()
        for t in topicList:
            pCount = self._cons.topicPartitionCount(t)
            if pCount == 0:
                logger.warning("Invalid topic '%s' encountered during metadata fetch", t)
            else:
                topics[t] = pCount
        return topics

    def _updateAssignments(self):
        topics = self._fetchAllTopicMetadata(self._conf.topics)

        assignments = self._coord.assignments(self._conf, self._memberId, self._assignments)

        changed = assignments.changeTopicPartitions(topics)

        if self._conf.configVersion > assignments.configVersion:
            assignments.changeMaxMembers(self._conf.maxGroupSize)
            assignments.configVersion = self._conf.configVersion
            changed = True

        if changed:
            assignments.version += 1
            self._coord.updateAssignments(self._meta(), assignments)

        return assignments

    def _isAssignmentChange(self, newAssignments: Assignments):
        if newAssignments is None:
            return False

        return self._assignments is None or newAssignments.version > self._assignments.version

    def _isMemberIdValid(self, verifiedMemberId: int) -> bool:
        """
        Validate that the recent member ID verification matches our current one.  This does
        not check whether the member ID is a valid ID based on the max group size. Reassignment
        should catch that change.

        Args:
            verifiedMemberId (int): The verfied member ID as returned from the coordinator

        returns (bool): True if the current member ID matches the verified one
        """
        return verifiedMemberId is not None and verifiedMemberId == self._memberId

    def _doReassignment(self, newAssignments: Assignments) -> bool:
        self._assignments = newAssignments

        if self._memberId is None:
            memberAssignments = None
        else:
            memberAssignments = self._assignments.getMemberAssignment(self._memberId)

        if memberAssignments is not None:
            try:
                self._cons.assign(memberAssignments)
                self._memberAssignment = memberAssignments
            except Exception:
                logger.exception('Failed to perform reassignment. assignments: %s', memberAssignments)
                return False
        else:
            logger.warning(
                'Current member ID not found in new assignments. memberId: %s, assignments: %s',
                self._memberId,
                newAssignments,
            )
            return False

        return True

    def _doLeave(self) -> bool:
        """
        Leave the group and update the consumer that we have left the group.

        returns (bool): True when the leave command has been successful
        """
        try:
            self._coord.leave(self._meta())
            self._memberId = None
            self._memberAssignment = None
            self._assignments = None
        except Exception:
            logger.exception('Failed to leave the group.')
            return False

        try:
            self._cons.onLeave()
        except Exception:
            logger.exception('consumer.onLeave() failed, ignoring.')

        return True

    def _doStop(self) -> bool:
        """
        Stop all activities for this consumer and cleanup resources.

        returns (bool): True if all stop activities finish cleanly
        """

        fail = False
        try:
            self._cons.close()
        except Exception:
            logger.exception('Failed to close consumer.')
            fail = True

        try:
            self._coord.stop()
        except Exception:
            logger.exception('Failed to close coordinator.')
            fail = True

        return not fail

    def _transitionState(self, newState: str):
        logger.debug('State transition. prevState: %s, nextState: %s', self._state, newState)
        self._state = newState

    def start(self):

        # TODO: catch errors
        self._cons.open()
        self._updateAssignments()

        # in loop only one state change per iteration, must always heartbeat until stopped
        # new -> joined -> assigned -> consuming -> stopping -> left -> stopped
        #
        # on new assignment:
        #   joined -> assigned
        #   assigned -> assigned
        #   consuming -> assigned
        #
        # on member id invalidated or assignment failure
        #   joined -> new
        #   assigned -> new
        #   consuming -> new
        #
        # on unrecoverable error
        #   fail fast, exit process
        #
        # on recoverable error
        #   retry with exponential decay
        #   fail after certain threshhold, exit process
        while self._state != self.STATE_STOPPED:

            # send heartbeat
            memberId = self._coord.heartbeat(self._meta())

            if self._state == self.STATE_LEFT:
                # this might fail but we're ready to exit the process anyways
                self._doStop()
                self._transitionState(self.STATE_STOPPED)

            elif self._state == self.STATE_STOPPING:
                if self._doLeave():
                    self._transitionState(self.STATE_LEFT)

            else:
                # new, joined, assigned, consuming states only
                if self._state == self.STATE_NEW:
                    memberId = self._coord.join(self._conf, self._memberId, self._assignments)

                    if memberId is not None:
                        self._memberId = memberId
                        self._transitionState(self.STATE_JOINED)
                        self._cons.onJoin()

                else:
                    # joined, assigned, consuming states only
                    # handle id loss or change
                    if not self._isMemberIdValid(memberId):
                        logger.warning(
                            f'Member ID is invalid! It has unexpectedly changed from {self._memberId} to {memberId}.'
                        )
                        self._transitionState(self.STATE_NEW)
                    else:

                        # check for updated assignments or get our initial assignment if just JOINED
                        newAssignments = self._coord.assignments(self._meta())

                        if self._isAssignmentChange(newAssignments):
                            success = self._doReassignment(newAssignments)

                            if success:
                                self._transitionState(self.STATE_ASSIGNED)
                            else:
                                self._transitionState(self.STATE_NEW)

                        elif self._state == self.STATE_ASSIGNED:
                            self._transitionState(self.STATE_CONSUMING)

                        if self._state == self.STATE_CONSUMING:
                            self._cons.poll()

    def stop(self):
        logger.info(
            'Stopping consumer membership. memberId: %s, assignment: %d', self._memberId, self._memberAssignment
        )
        self._transitionState(self.STATE_STOPPING)
        while self._state != self.STATE_STOPPED:
            time.sleep(1)
        logger.info('Completed stop')

    def state(self):
        return self._state
