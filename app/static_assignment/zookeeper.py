import logging
import time
from typing import Optional
from typing import Union

import ujson
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import ConnectionLoss
from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import SessionExpiredError
from kazoo.recipe.watchers import DataWatch
from static_assignment.assignments import Assignments
from static_assignment.assignments import MemberId
from static_assignment.group import StaticCoordinator
from static_assignment.group import StaticMemberMeta

logger = logging.getLogger(__name__)


class ZkCoordinator(StaticCoordinator):
    @staticmethod
    def fromGroup(zkConnect: str, group: str) -> Union['ZkCoordinator']:
        prePath = '/static_assignment/' + group
        assignmentPath = prePath + '/assignment'
        membersPath = prePath + '/members'
        return ZkCoordinator(zkConnect, membersPath, assignmentPath)

    def __init__(self, zkConnect, membersPath, assignmentsPath):
        """Zookeeper implementation of `StaticCoordinator`"""

        logger.info('ZKCoordinator starting with, membersPath=%s, assignmentsPath=%s', membersPath, assignmentsPath)
        self._zkConnect = zkConnect

        self._membersPath = membersPath
        self._membersPathEnsured = False

        self._assignmentsPath = assignmentsPath
        self._assignmentsPathEnsured = False
        self._currentAssignment = None
        self._memberMetaData = None

        self.zk = KazooClient(hosts=zkConnect)
        self.zk.add_listener(self._zkListener())
        self._memberId: Optional[MemberId] = None

    def _zkListener(self):
        def listener(state):
            if state == KazooState.LOST:
                self._memberId = None
                self._currentAssignment = None
            if state == KazooState.CONNECTED:
                self._establishSession()

        return listener

    def _establishSession(self):

        logger.debug('Establishing session')

        # add watch for assignment updates
        def watchAssignments(data, stat, event):
            logger.debug('Assignment watch triggered. event: %s', event)
            self._currentAssignment = self._processAssignmentsData(data)

        DataWatch(self.zk, watchAssignments)
        logger.debug('Session established')

    def _ensureAssignmentsPath(self):
        logger.debug('ensuring assignments path. ensured: %s', self._assignmentsPathEnsured)
        if not self._assignmentsPathEnsured:
            self.zk.ensure_path(self._assignmentsPath)
            self._assignmentsPathEnsured = True

    def _fetchAssignments(self) -> Assignments:
        if self._currentAssignment is None:
            logger.debug('getting assignments path %s', self._assignmentsPath)
            data, _ = self.zk.get(self._assignmentsPath)
            self._currentAssignment = self._processAssignmentsData(data)
            logger.debug('assignments received')

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
                logger.info('Updating member meta data. oldMeta: %s, newMeta: %s', selfDict, newDict)
                self.zk.set(path, self._encodeMemberData(meta))

    def updateAssignments(self, meta: StaticMemberMeta, newAssignments: Assignments):
        logger.debug('updating assignment')
        self.zk.retry(self._innerUpdateAssignment, newAssignments)
        logger.debug('assignment update complete')

    def _innerUpdateAssignment(self, assignment: Assignments):
        self._ensureAssignmentsPath()
        self.zk.set(self._assignmentsPath, assignment.asJson().encode('utf-8'))

    def leave(self, meta: StaticMemberMeta):
        self.zk.retry(self._innerLeave)

    def _innerLeave(self):
        path = self._createPath()
        if path is not None:
            self.zk.delete(path)
            self._memberId = None

    def join(self, meta: StaticMemberMeta):

        asgns = self._fetchAssignments()
        if asgns is None:
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

            # TOdo: should there sleep in here?
            try:
                self.zk.create(memberIdPath, memberData, ephemeral=True)
                foundMid = mid
                logging.debug('Found available member id. memberId: %s', mid)
                break
            except NodeExistsError:
                # move onto the next node
                logger.debug('Member id (%s) already exists moving to next.', mid)
            except (ConnectionLoss, SessionExpiredError):
                time.sleep(1)

        self._memberMetaData = meta
        return foundMid

    def assignments(self, meta: StaticMemberMeta) -> Assignments:
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
        self._ensureAssignmentsPath()
