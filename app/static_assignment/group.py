import logging
import os
import socket
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from static_assignment.assignments import Assignments
from static_assignment.assignments import AssignmentVersion
from static_assignment.assignments import MemberAssignment
from static_assignment.assignments import MemberId
from static_assignment.assignments import Topics

logger = logging.getLogger(__name__)


class StaticConsumer:
    """StaticMembership consumer interface.

    Implement this class and all methods that are not prefixed with 'on' (those methods are optional)
    to include your consumer in the StaticMembership strategy for managing consumer group partition assignments.

    The required methods by default raise NotImplementedError if invoked.
    """

    def topicPartitionCount(self, topic):
        """Looks up the partition count for the given topic.

        Discovers how many partitions the given topic has. If the topic does not exist then 0 is returned.

        Args:
            topic (str): The name of the topic to find the partition count for.

        Returns:
            int: The partition count or 0 if the topic is invalid
        """
        raise NotImplementedError

    def poll(self):
        """Performs assignment polling and data processing.

        Invoked repeatedly to allow the consumer consume data and perform any related data processing
        against its assigned partitions. If commits are being handled manually then the commit action
        should be taken also.

        This method must not block indifinitely since it is called from the main membership loop and
        prevents any further state changes to the membershp lifecycle from happening. This includes assignment
        updates, heartbeats, graceful shutdowns and membership updates.

        Any error raised from this method will cause the consumer to leave the group and attempt to rejoin again.
        """
        raise NotImplementedError

    def assign(self, assignments: MemberAssignment):
        """Called when partition assignments must be replaced.

        This method is invoked whenever assignments have changed for this consumer. This happens after joining the
        group and also whenever assignments have been updated. If for whatever reason the assignment updates fail then
        an error should be raised. This will trigger the consumer to leave the group and attempt to rejoin.

        This a complete replacement of the assignments no prior assignments should be kept.

        Args:
            assignments (MemberAssignment): The assignments that should be applied so that the `poll()` method will return back data from the given topics and partitions.
        """
        raise NotImplementedError

    def open(self):
        """When invoked the consumer should establish any network connections or resource allocations.

        Invoked prior to doing anything else. The consumer should establish itself and connect to kafka. If an error is encountered then this will cause a
        fast failure and an outside process will need to perform retries.
        """
        raise NotImplementedError

    def close(self):
        """When invoked the consumer should close down and cleanup any resources it has allocated.

        Invoked when the process is being shutdown. Gives the consumer a chance to gracefully close itself down including
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
    @staticmethod
    def fromDict(confDict: Dict[str, Any]) -> Optional['StaticConfig']:
        return StaticConfig(
            hostId=confDict.get('hostId'),
            topics=confDict.get('topics', list()),
            group=confDict.get('group'),
            maxGroupSize=int(confDict.get('maxGroupSize', 0)),
            configVersion=confDict.get('configVersion'),
            zkConnect=confDict.get('zkConnect'),
            kafkaConnect=confDict.get('kafkaConnect'),
            more=confDict.get('more', dict()),
        )

    def __init__(
        self,
        hostId=None,
        topics: List[str] = None,
        group: str = None,
        maxGroupSize=None,
        configVersion=None,
        zkConnect=None,
        kafkaConnect=None,
        more: Dict[str, Any] = dict(),
    ):
        """Configuration for static membership.

        Can be used for configuration throughout the application but has parameters specified that are known.

        Attributes:
            hostId=None (str): A unique identifier for the host instance of this process. If `None` then hostname combined with the process ID will be used.
            topics=None (List[str]): The target topics for the consumers to consume from
            group=None (str): A name for the consumer group that can be used to isolate assignments
            maxGroupSize=None (int): The maximum membership count expected for the static group
            configVersion=None (int): The version of the configuration, used to prevent reverting of assignments
            maxPollInterval=5000 (int): The maximum number of milliseconds between heartbeats before a member is considered unhealthy
            zkConnect=None (str): A standard connection string for zookeeper, host:port,host2:port,host3:port
            kafkaConnect=None (str): Standard kafka bootstrap servers string, [proto://]host:port,[proto://]host2:port,[proto://]host3:port
            more=dict() (dict): Any additional configuration that may be needed for custom `StaticConsumers` or `StaticCoordinators`
        """
        if hostId is None:
            self.hostId = f'{socket.gethostname()}-{os.getpid()}'
        else:
            self.hostId = hostId

        self.topics = topics
        self.group = group
        self.maxGroupSize = maxGroupSize
        self.configVersion = configVersion
        self.zkConnect = zkConnect
        self.kafkaConnect = kafkaConnect
        self.more = more


class StaticMemberMeta:
    def __init__(self, hostId: str, memberId: MemberId = None, assignmentVersion: AssignmentVersion = None):
        """ Information about a static member that is used to inform the coordinator of about it.

        Attributes:
            hostId (str): The member's hostId
            memberId (MemberId): The members ID
            assignmentVersion (AssignmentVersion): The current assignment version the member is using.
        """
        self.hostId = hostId
        self.memberId = memberId
        self.assignmentVersion = assignmentVersion

    def asDict(self):
        """ Convert this object into a dictionary containing all values found within.

        Useful for serialization/deserialization operations.
        """
        if self.assignmentVersion is not None:
            configVersion = self.assignmentVersion.configVersion
            version = self.assignmentVersion.version
        else:
            configVersion = None
            version = None

        return {'hostId': self.hostId, 'assignment': {'configVersion': configVersion, 'version': version}}


class StaticCoordinator:
    """StaticCoordinator coordinator interface.

    Implement this class and all methods to create your own coordinator to be used StaticMembership
    strategy for managing consumer group partition assignments.

    The required methods by default raise NotImplementedError if invoked.
    """

    def updateAssignments(self, meta: StaticMemberMeta, newAssignments: Assignments):
        """Update the current assignments with the new assignment.

        The coordinator must simply take the new assignments and make them available to the rest of the
        group, updating them when they are ready. It is not responsible for validating them in anyway or
        should it change them.

        Args:
            meta (StaticMemberMeta): Meta data about the member invoking this method.
            newAssignments (Assignments): The new assignments that will replace the current ones.
        """
        raise NotImplementedError

    def leave(self, meta: StaticMemberMeta):
        """Leave the group and give up the associated member ID.

        Leave the group and give up the current memberId and assignments.

        Calling this method when the member is not part of the group or has already left should
        not raise an exception.

        Args:
            meta (StaticMemberMeta): Meta data about the member invoking this method.
        """
        raise NotImplementedError

    def join(self, meta: StaticMemberMeta) -> Optional[MemberId]:
        """Attempt to join the group.

        Join the consumer group attempting to find an unassigned `MemberId`.

        This method should be idempotent meaning that if the member is already assigned and actively
        still a member then no action should be taken and the existing member ID should be returned.

        Args:
            meta (StaticMemberMeta): Meta data about the member invoking this method.
        Returns:
            Optional[MemberId]: The consumers granted member ID or `None` if a member id could not be assigned.

        """
        raise NotImplementedError

    def assignments(self, meta: StaticMemberMeta) -> Optional[Assignments]:
        """Fetch the current assignments for the group.

        Fetches the most current version of the assignments. In the case where the most current
        version is not accessible then None should be returned.

        Args:
            meta (StaticMemberMeta): Meta data about the member invoking this method.
        Returns:
            Assignments: The current assignments or None if they are not accessible for any reason
        """
        raise NotImplementedError

    def heartbeat(self, meta: StaticMemberMeta) -> Optional[MemberId]:
        """Send a heartbeat indicating liveliness and providing any updated member information.

        A heartbeat must be performed as part of the membership contract on a regular basis. If this is
        not done then the member information may become stale and the member may eventually be forced out
        of the group.

        This call also returns the current member ID assigned to the consumer as understood by the coordinator. If
        the returned `MemberId` is not the same as the consumer's current one or it is `None` then the consumer should
        assume that the coordinator is not in sync with the rest of the group and has lost it's membership.

        Args:
            meta (StaticMemberMeta): Meta data about the member invoking this method.
        Return
            MemberId: The memberId currently assigned to the member as known by the coordinator.
        """
        raise NotImplementedError

    def start(self):
        """Start the coordinator allocating all necessary resources.

        Start the coordinator. On this call it should make any resource allocations,
        network connections and get into a consistent state with the rest of the consumer
        group.

        This should not throw an exception if called after already being started.

        If an error is raised then the calling logic should either perform retries or fail fast.
        """
        raise NotImplementedError

    def stop(self):
        """Stop the coordinator releasing any allocated resources it may have.

        Stop this coordinator and have it cleanup any related resource allocations.
        Calling this on an already stopped or unstarted coordinator should not throw an
        exception.

        If an error is raised then the calling logic should continue its shutdown procedure or fail fast.
        """
        raise NotImplementedError


class StaticMembership:
    def __init__(self, config: StaticConfig, staticConsumer: StaticConsumer, coordinator: StaticCoordinator):
        """A membership to the static group.

        Represents membership to a statically assigned kafka consumer group.

        General usage is done by first implementing a `StaticConsumer` and `StaticCoordinator`. Once those
        are supplied then this membership is activated by calling ``start()

        The start method blocks until the membership is cancelled (stopped). This can be done by calling the ``stop()``
        method. Generally this is started in a separate thread.

        Attributes:
            config (StaticConfig): The configuration for this membership.
            staticConsumer (StaticConsumer): An instance of a static consumer.
            coordinator (StaticCoordinator): An instance of a static coordinator.

        Todo:
            * Implement a retry mechanism to short circuit recovery attempts after a certain point.
            * Add a callback hook for state change events.
            * Add a heartbeat limiter to prevent hammering the coordinator.
        """
        self.STATE_STOPPED = 'stopped'
        self.STATE_STOPPING = 'stopping'
        self.STATE_NEW = 'new'
        self.STATE_JOINED = 'joined'
        self.STATE_LEFT = 'left'
        self.STATE_ASSIGNED = 'assigned'
        self.STATE_CONSUMING = 'consuming'

        # Todo: rename error count
        self._errorCount = 0
        self._state: Optional[str] = None
        self._assignments: Optional[Assignments] = None
        self._memberId: Optional[MemberId] = None
        self._memberAssignment: Optional[MemberAssignment] = None
        self._conf = config
        self._cons = staticConsumer
        self._coord = coordinator

    def _meta(self):
        # Generates member meta data from current state
        aVersion = None
        if self._assignments is not None:
            aVersion = self._assignments.assignmentVersion()

        return StaticMemberMeta(self._conf.hostId, self._memberId, aVersion)

    def _fetchAllTopicMetadata(self, topicList: List[str]) -> Topics:
        # Fetch all topic partition counts from the `StaticConsumer` at once.
        topics = dict()
        for t in topicList:
            pCount = self._cons.topicPartitionCount(t)
            if pCount == 0:
                logger.warning("Invalid topic '%s' encountered during metadata fetch", t)
            else:
                topics[t] = pCount
        return topics

    def _updateAssignments(self):
        # potentially generate new member assignments based on our `StaticConfig`
        topics = self._fetchAllTopicMetadata(self._conf.topics)

        assignments = self._coord.assignments(self._meta())
        # this may be the first run ever for this group so no assignments are available
        if assignments is None:
            assignments = Assignments(
                group=self._conf.group,
                maxMembers=self._conf.maxGroupSize,
                topics=topics,
                configVersion=self._conf.configVersion,
                version=0,
            )
            changed = True
        else:

            # partition counts can change outside of the configuration updates
            changed = assignments.changeTopicPartitions(topics)

            # Update the assignments if our configVersion is greater than the one
            # provided by the coordinator
            if self._conf.configVersion > assignments.configVersion:
                # may or may not cause a recalculation of the assignments
                assignments.changeMaxMembers(self._conf.maxGroupSize)

                # this must be updated though
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
        # Reassigns the consumer given the new assignments if possible.
        # if false is returned then reassignment failed and the consumer may be
        # out of sync with the rest of the group
        self._assignments = newAssignments

        if self._memberId is None:
            memberAssignments = None
        else:
            memberAssignments = self._assignments.getMemberAssignment(self._memberId)

        if memberAssignments is not None:
            try:
                logger.debug('Received new assignment: %s', memberAssignments)
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
        # Leave the group and update the consumer that we have left the group.
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
        # Stop all activities for this consumer and cleanup resources.
        # Will return False if any stop activities fail but regardless all activities will be done.
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
        # Single point for catching transitions
        logger.debug('State transition. prevState: %s, nextState: %s', self._state, newState)
        self._state = newState

    def start(self):
        """Start this membership and immediately begin consuming if possible.

        Starts the membership lifecycle which includes managing the membership and updating the consumer
        when it changes. This method blocks until the membership is stopped.

        You can think of the membership as having a 'tick' associated to it and on each tick 2 things
        occur, first a heartbeat then an action. The action can either be a state associated action such as
        poll for data or it could be state transition action such as transitioning from being joined to being
        assigned.

        Below is an attempt to describe the state diagram::

            new -> joined -> assigned -> consuming -> stopping -> left -> stopped

        When a new/updated assignment is received these are the transitions::

            [joined, assigned, consuming] -> assigned

        When the current member id is invalidated or the assignment fails::

            [joined, assigned, consuming] -> new

        When an unrecoverable error occurs the process attempts to stop cleanly::

            [new, joined, assigned, consuming] -> stopping

        When a recoverable error occurs the membership reverts back to new and attempts
        to recover through the normal lifecycle.
        """

        self._cons.open()
        self._coord.start()
        self._updateAssignments()
        self._transitionState(self.STATE_NEW)

        while self._state != self.STATE_STOPPED:

            # send heartbeat
            # logger.debug("Sending heartbeat")
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
                    logger.debug('Attempting to join')
                    memberId = self._coord.join(self._meta())

                    if memberId is not None:
                        self._memberId = memberId
                        self._transitionState(self.STATE_JOINED)
                        self._cons.onJoin()
                    else:
                        # let it cool down for a bit
                        time.sleep(1)

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
        """Stop this membership and associated `StaticConsumer` and `StaticCoordinator` gracefully.

            Stop this membership and release any resources associated with it including the consumer and coordinator.
            If any failures occur during this they will be ignored and this will finish regardless.

            Stopping a membership that has not been started or has already been stopped will not induce an error.
        """
        if self._state is not None and self._state != self.STATE_STOPPED:
            logger.info(
                'Stopping consumer membership. memberId: %s, assignment: %s', self._memberId, self._memberAssignment
            )
            self._transitionState(self.STATE_STOPPING)
            while self._state != self.STATE_STOPPED:
                time.sleep(1)
        else:
            self._doLeave()
            self._doStop()

        logger.info('Completed stop')

    def state(self) -> Optional[str]:
        """Return the current state of this membership"""
        return self._state
