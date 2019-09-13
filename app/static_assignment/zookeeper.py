import logging
import time
from typing import Optional

import ujson
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import ConnectionLoss
from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import SessionExpiredError
from kazoo.recipe.watchers import DataWatch

from app.static_assignment.assignments import Assignments
from app.static_assignment.assignments import MemberId
from app.static_assignment.group import StaticCoordinator
from app.static_assignment.group import StaticMemberMeta

logger = logging.getLogger(__name__)


class ZkCoordinator(StaticCoordinator):
    @staticmethod
    def fromGroup(zkConnect: str, group: str) -> 'ZkCoordinator':
        """Convenience method for instantiation using conventional paths based on group.

        The path convention is:

            /static_assignment/[group]/assignments
            /static_assignment/[group]/members

        Args:
            zkConnect (str): Comma-separated list of hosts to connect to (e.g. 127.0.0.1:2181,127.0.0.1:2182,[::1]:2183).
            group (str): The name of the consumer group this coordinator belongs to. Must not be None.
        """

        if group is None or len(group.strip()) == 0:
            raise ValueError('ZkCoordinator: Invalid `group` argument, it must not be None or blank.')

        prePath = f'/static_assignment/{group.strip()}'
        assignmentPath = f'{prePath}/assignment'
        membersPath = f'{prePath}/members'
        return ZkCoordinator(zkConnect, membersPath, assignmentPath)

    def __init__(self, zkConnect: str, membersPath: str, assignmentsPath: str):
        """Zookeeper implementation of `StaticCoordinator`

        Args:
            zkConnect (str): Comma-separated list of hosts to connect to (e.g. 127.0.0.1:2181,127.0.0.1:2182,[::1]:2183).
            membersPath (str): Zookeeper path at which members will create ephemeral nodes asserting their ID.
            assignmentsPath (str): Zookeeper path at which the current assignments are kept.
        """

        for val, name in ((zkConnect, 'zkConnect'), (membersPath, 'membersPath'), (assignmentsPath, 'assignmentsPath')):
            if val is None or len(val.strip()) == 0:
                raise ValueError(f'ZkCoordinator: Invalid `{name}` argument, it must not be None or blank')

        logger.info('ZKCoordinator starting with, membersPath=%s, assignmentsPath=%s', membersPath, assignmentsPath)
        self._zkConnect = zkConnect

        self._membersPath = membersPath
        self._membersPathEnsured = False

        self._assignmentsPath = assignmentsPath
        self._assignmentsPathEnsured = False
        self._currentAssignment = None
        self._assignmentsWatcher = None
        self._memberMetaData: Optional[StaticMemberMeta] = None

        self.zk = KazooClient(hosts=zkConnect)
        self.zk.add_listener(self._zkListener())
        self._memberId: Optional[MemberId] = None

    def _zkListener(self):
        def listener(state):
            if state == KazooState.LOST:
                self._memberId = None
                self._currentAssignment = None

        return listener

    def _establishSession(self):

        if self._assignmentsWatcher is None:
            # add watch for assignment updates
            def watchAssignments(data, stat, event):
                self._currentAssignment = self._processAssignmentsData(data)
                logger.info('Assignment update received. | assignments= %s', self._currentAssignment)

            self._ensureAssignmentsPath()
            self._assignmentWatcher = DataWatch(self.zk, self._assignmentsPath, watchAssignments)

    def _ensureAssignmentsPath(self):
        if not self._assignmentsPathEnsured:
            self.zk.ensure_path(self._assignmentsPath)
            self._assignmentsPathEnsured = True

    def _fetchAssignments(self) -> Optional[Assignments]:
        return self._currentAssignment

    def _processAssignmentsData(self, rawData):
        if rawData is not None:
            return Assignments.fromJson(rawData.decode('utf-8'))

    def _ensureMembersPath(self):
        if not self._membersPathEnsured:
            self.zk.ensure_path(self._membersPath)
            self._membersPathEnsured = True

    def _createPath(self, altMemberId: MemberId = None):
        mid = self._memberId

        if altMemberId is not None:
            mid = altMemberId

        if mid is not None:
            return f'{self._membersPath}/{mid}'

        return None

    def _encodeMemberData(self, meta: StaticMemberMeta):
        return ujson.dumps(meta.asDict()).encode('utf-8')

    def _compareAndUpdateMemberData(self, meta: StaticMemberMeta):
        newDict = None
        selfDict = None
        if self._memberMetaData is not None and meta is not None:
            selfDict = self._memberMetaData.asDict()
            newDict = meta.asDict()

            isDiff = (
                selfDict['hostId'] != newDict['hostId']
                or selfDict['assignment']['configVersion'] != newDict['assignment']['configVersion']
                or selfDict['assignment']['version'] != newDict['assignment']['version']
            )
        else:
            isDiff = True

        if isDiff:
            self._memberMetaData = meta

            path = self._createPath()
            if path is not None:

                def cb(async_obj):
                    try:
                        async_obj.get()
                        logger.info('Member meta data updated. | metaData=%s', meta)
                    except (ConnectionLoss, SessionExpiredError):
                        logger.exception('Failed to update member meta data.')

                self.zk.set_async(path, self._encodeMemberData(meta)).rawlink(cb)

    def updateAssignments(self, meta: StaticMemberMeta, newAssignments: Assignments):
        self.zk.retry(self._innerUpdateAssignment, newAssignments)

    def _innerUpdateAssignment(self, assignment: Assignments):
        self._ensureAssignmentsPath()
        self.zk.set(self._assignmentsPath, assignment.asJson().encode('utf-8'))
        logger.info('Assignments updated. | assignments=%s', assignment)

    def leave(self, meta: StaticMemberMeta):
        self.zk.retry(self._innerLeave)

    def _innerLeave(self):
        path = self._createPath()
        if path is not None:
            try:
                self.zk.delete(path)
            except (ConnectionLoss, SessionExpiredError):
                logger.exception(
                    'Failed to relinquish member ID, '
                    "will assume ephemeral node will expire on it's own. "
                    '| memberId=%s',
                    self._memberId,
                )
            self._memberId = None

    def join(self, meta: StaticMemberMeta):

        asgns = self._fetchAssignments()
        if asgns is None:
            logger.warning('Cannot join a group without assignments. | assignmentsPath=%s', self._assignmentsPath)
            return None

        if self._memberId is None:
            self._memberId = self._inner_join(meta, asgns.maxMembers)

        return self._memberId

    def _inner_join(self, meta: StaticMemberMeta, maxMembers: int) -> Optional[MemberId]:
        idList = range(maxMembers)
        memberData = self._encodeMemberData(meta)
        self._ensureMembersPath()

        foundMid = None

        for mid in idList:
            memberIdPath = self._createPath(mid)

            try:
                self.zk.create(memberIdPath, memberData, ephemeral=True)
                foundMid = mid
                logging.debug('Member id acquired. | memberId=%s', mid)
                break
            except NodeExistsError:
                # move onto the next node
                logger.debug('Member id already taken moving to next. | memberId=%s', mid)
            except (ConnectionLoss, SessionExpiredError):
                logger.exception('Member id acquisition attempt failed with error.')
                time.sleep(1)

        self._memberMetaData = meta
        return foundMid

    def assignments(self, meta: StaticMemberMeta) -> Optional[Assignments]:
        self._compareAndUpdateMemberData(meta)
        return self._fetchAssignments()

    def heartbeat(self, meta: StaticMemberMeta) -> Optional[MemberId]:
        self._compareAndUpdateMemberData(meta)
        return self._memberId

    def stop(self):
        self.zk.stop()
        self.zk.close()

    def start(self):
        self.zk.start()
        self._establishSession()
