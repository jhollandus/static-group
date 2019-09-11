import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import ujson

logger = logging.getLogger(__name__)

#: A mapping of topics to assigned partitions.
#: This type is used to maintain a map of assignments for a particular member. It is
#: constructed soley of built-in types to ease serialization/deserialization.
TopicAssignment = Dict[str, List[int]]

#: A mapping of topics to their partition count.
Topics = Dict[str, int]

#: The identifier used to assign `TopicAssignments` to a member of the consumer group.
MemberId = int


class MemberAssignment:
    @staticmethod
    def fromPrimitive(pRep) -> Optional['MemberAssignment']:
        """
        Converts the primitive representation as produced by `toPrimitive()`
        back into a MemberAssignment.

        returns (MemberAssignment): None returned if the input does not conform
        """

        valid = True
        valid = (
            pRep is not None
            and 'memberId' in pRep
            and 'topics' in pRep
            and isinstance(pRep['topics'], dict)
            and isinstance(pRep['memberId'], int)
        )

        if valid:
            for t, pList in pRep['topics'].items():
                if not valid:
                    break

                if not isinstance(pList, list):
                    valid = False
                else:
                    for p in pList:
                        if not isinstance(p, int):
                            valid = False
                            break

        if valid:
            return MemberAssignment(int(pRep['memberId']), pRep['topics'])
        else:
            logger.warning('Failed to convert primitive member assignment to MemberAssignment. prim: %s', pRep)
            return None

    def __init__(self, memberId: MemberId, topics: Optional[TopicAssignment] = None):
        """Manages the assignments for a specific member of the assignment group.

        Instances of this class are used to manage the topic-partition assignments for a
        specific member identified by a `MemberId`.

        When using this class one can iterate over the assignments in a predetermined order
        that should stay consistent regardless of what order the assignments were assigned.

        Support for serialization and deserialization is provided by the class method `MemberAssignment.fromPrimitive()`
        and the instance method `toPrimitive()` which can be used with tools for working with different serialization formats
        (json and yaml for instance).

        Attributes:
            memberId (MemberId): The member these assignments are for.
            topics (TopicAssignment): The actual topic-partition assignments.
        """

        self.memberId = memberId
        self.topics: TopicAssignment = topics if topics is not None else dict()

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

    def toPrimitive(self) -> Dict[str, Any]:
        """A representation consisting only of built-in types.

        Return a representation consisting of only built in types.
        This is useful for JSON serdes.
        """
        return {'memberId': self.memberId, 'topics': self.topics}

    def totalAssignments(self):
        """The total number of partition assignments currently assigned to this member."""
        return sum([len(p) for t, p in self.topics.items()])

    def assign(self, topic: str, partition: int):
        """Assigns the topic and partition number to this member.

        Assigns the topic-partition combination to this member. It does not validate
        the correctness of the assignment such as verifying the topic or partition number exists.

        Args:
            topic (str): Name of the topic to be assigned
            partition (int): The topic partition number to be assigned
        """
        if topic not in self.topics:
            self.topics[topic] = []

        if partition not in self.topics[topic]:
            self.topics[topic].append(partition)


class AssignmentVersion:
    def __init__(self, version: int, configVersion: int, group: str):
        """A small token that represents the version of a group's `Assignments`.

        Indicates the version of the assignments associated to a static membership
        group.

        Attributes
            version (int): The version of the assignment, this is a monotonically incremementing integer that changes whenever the Assignments change.
            configVersion (int): This version prevents older process configurations from reverting newer ones. It should always increase over time.
            group (str): The name of the consumer group, allows assignments to be isolated based on group.
        """

        self.version = version
        self.configVersion = configVersion
        self.group = group


class Assignments:
    @staticmethod
    def fromJson(jsonData: str) -> Optional['Assignments']:
        """Deserializes the given JSON assuming it was generated by `asJson()`.

        Deserializes JSON as produced by the asJson() method into an Assignments object.

        Args:
            jsonData (str): The serialized Assignments JSON

        Returns:
            Assignment: The deserialized Assignment or None if the jsonData is invalid
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
        """Represnets and manages all assignments for a group.

        This represents all calculated assignments for a group. It is statically generated and
        creates assignments for members from `MemberId` 0 to `maxMembers`. It does not assign
        any particular consumer instance to the assignments, that is left to another part of the
        program.

        Supports identifying upon criteria changes (maxMembers, topics) whether the assignment table
        will change and optionally recalculates all assignments according to the new values.

        Attributes:
            group (str): The group name these assignments belong to.
            maxMembers (int): The maximum number of members expected for this group.
            topics (Topics): A list of topics to assign across the members.
            configVersion (int): A version number that should always increase in value
                when changes occur due to release time changes. The unix epoch at the
                time of release will do. This allows us to identify older assignments from
                newer ones during deployment time.
            version (int): A monotonically increasing version number that changes with every update,
                including those triggered by release changes and those at runtime, such as partition
                count updates.
            memberAssignments (List[MemberAssignment]): All calculated member assignments with the
                list index also representing the `MemberId`.
            doReassign=True (bool): Whether to perform reassignment calculations at init or not.
        """
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
        return f'{{assignments: {self.memberAssignments}}}'

    def __repr__(self):
        return self.__str__()

    def _reassign(self):
        calculator = AssignmentCalculator(self.maxMembers, self.topics)
        self.memberAssignments = calculator.generateAssignments(self)

    def asJson(self) -> str:
        """
        Serializes this object into JSON suitable for deserialization using
        the static method `fromJson()`.

        Returns:
            str: The JSON
        """
        j: Dict[str, Any] = {}
        j['group'] = self.group
        j['topics'] = self.topics
        j['maxMembers'] = self.maxMembers
        j['version'] = self.version
        j['configVersion'] = self.configVersion
        j['memberAssignments'] = [ma.toPrimitive() for ma in self.memberAssignments]

        return ujson.dumps(j)

    def assignmentVersion(self) -> AssignmentVersion:
        """Generate an `AssignmentVersion` token from this.

        The generated `AssignmentVersion` may contain many `None` values based on the current
        state of the assignment.

        Returns:
            AssignmentVersion: Generated version token for this.
        """
        return AssignmentVersion(self.version, self.configVersion, self.group)

    def changeMaxMembers(self, maxMembers: int) -> bool:
        """Update the maximum members belonging to this group's assignments

        Updates the maximum number of members that may belong to this group. If this
        update causes a change to the assignments then True will be returned.

        Args:
            maxMembers (int): The new maximum group size, must be greater than 0

        Returns :
            bool: True if the change triggered a recalculation of assignments
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
        """Replace the topic-partitions that should be assigned.

        Updates the topic partitions that should be assigned across the members. If
        this change causes an assignment redistribution then True is returned.

        This is a complete replacement, all previous topic-partitions will be removed.

        Args:
            topics (Topics): Updated dictionary of topics with partition counts

        Returns:
            bool: True if a change in assignments occurred
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

    def getMemberAssignment(self, memberId: MemberId) -> Optional[MemberAssignment]:
        """Get assignments for a particular member.

        Fetches the assignments for the given member ID.

        Args:
            memberId (int): The member ID to find the assignments for

        Returns:
            MemberAssignment: The assignments for the member ID or None if the member ID is unknown or unassigned
        """
        if len(self.memberAssignments) > memberId:
            return self.memberAssignments[memberId]
        else:
            return None


class AssignmentCalculator:
    def __init__(self, maxSize: int, topics: Topics):
        """Calculates group assignments.

        Calculate group assigments based on the given criteria.

        Args:
            maxSize (int): The maximum number of assignments to calculate
            topics (Topics): A dictionary of topic names to partition counts that need assigning
        """

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
        return sum([p for t, p in self.topics.items()])

    def generateAssignments(self, prevAssignments: Assignments = None) -> List[MemberAssignment]:
        """Generates all assignments.

        Generates a new list of assignments for all members based on the state of the calculator. Can take into
        consideration the max group size, topics to distribute and previous assignments.

        Args:
            prevAssignments (Assignments): Can be used to help determine efficient redistribution of assignments

        Returns:
            List[MemberAssignment]: The calculated list of all member assignments with the list index also being the `MemberId`.
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
