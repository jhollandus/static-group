from kazoo.client import KazooClient
from kazoo.client import KazooState
from static_assignment.static_group import Assignments
from static_assignment.static_group import StaticCoordinator
from static_assignment.static_group import StaticMemberMeta


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
        self.lastMemberId = None

        data, stat = self.zk.get(self.assignmentsPath)
        self.currentAssignment = data.decode('utf-8')

        self.zk.get_children(self.memberPath)

        @self.zk.DataWatch(self.assignmentsPath)
        def watchAssignments(data, stat, event):
            self.currentAssignment = data.decode('uft-8')

        @self.zk.ChildrenWatch(self.memberPath)
        def watch_children(children):
            print('Children are now: %s' % children)

    def _establishSession(self):
        # attempt to establish ephemeral node if lastMemberId is known
        pass

    def _zkListener(self):
        def listener(state):
            if state == KazooState.LOST:
                self.lastMemberId = self.memberId
                self.memberId = None
                self.currentAssignment = None
            if state == KazooState.CONNECTED:
                self._establishSession()

        return listener

    def updateAssignments(self, meta: StaticMemberMeta, newAssignments: Assignments):
        raise NotImplementedError

    def leave(self, meta: StaticMemberMeta):
        raise NotImplementedError

    def join(self, meta: StaticMemberMeta) -> int:
        raise NotImplementedError

    def assignments(self, meta: StaticMemberMeta) -> Assignments:
        raise NotImplementedError

    def heartbeat(self, meta: StaticMemberMeta) -> int:
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError
