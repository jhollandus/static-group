import pytest

from app.static_assignment.assignments import Assignments
from app.static_assignment.assignments import AssignmentVersion
from app.static_assignment.group import StaticConfig
from app.static_assignment.group import StaticConsumer
from app.static_assignment.group import StaticCoordinator
from app.static_assignment.group import StaticMemberMeta
from app.static_assignment.group import StaticMembership


@pytest.fixture()
def staticConfigDict():
    return {
        'host.id': 'hostId',
        'topics': ['t1', 't2'],
        'group': 'group',
        'max.group.size': 8,
        'config.version': 1234,
        'zk.connect': 'localhost:2181',
        'kafka.connect': 'localhost:9092',
        'more': {'foo': 'bar'},
    }


@pytest.fixture()
def mid():
    return 0


@pytest.fixture()
def staticConfig(staticConfigDict):
    return StaticConfig.fromDict(staticConfigDict)


@pytest.fixture()
def mockedMembership(mocker, staticConfig):
    mockCoord = mocker.MagicMock()
    mockCons = mocker.MagicMock()
    sm = StaticMembership(staticConfig, mockCons, mockCoord)

    return (mockCons, mockCoord, sm)


@pytest.fixture()
def validMockedMembership(assignments, mockedMembership):
    def partitionCount(t):
        return assignments.topics[t]

    cons, coord, sm = mockedMembership
    sm._conf.configVersion = assignments.configVersion
    sm._conf.topics = assignments.topics.keys()
    cons.topicPartitionCount.side_effect = partitionCount
    coord.assignments.return_value = assignments

    return (assignments, cons, coord, sm)


def test_static_consumer_raises_not_implemented():
    stcon = StaticConsumer()

    with pytest.raises(NotImplementedError):
        stcon.assign(None)

    with pytest.raises(NotImplementedError):
        stcon.close()

    with pytest.raises(NotImplementedError):
        stcon.open()

    with pytest.raises(NotImplementedError):
        stcon.poll()

    with pytest.raises(NotImplementedError):
        stcon.topicPartitionCount('')

    try:
        stcon.onJoin()
        stcon.onLeave()
    except Exception:
        pytest.fail('Exception thrown when not expected.')


def test_static_config_from_dict(staticConfigDict):
    d = staticConfigDict

    stcfg = StaticConfig.fromDict(d)
    assert stcfg.hostId == d['host.id']
    assert stcfg.topics == d['topics']
    assert stcfg.group == d['group']
    assert stcfg.maxGroupSize == d['max.group.size']
    assert stcfg.configVersion == d['config.version']
    assert stcfg.zkConnect == d['zk.connect']
    assert stcfg.kafkaConnect == d['kafka.connect']
    assert stcfg.more == d['more']


def test_static_config_from_no_hostId(staticConfigDict):
    d = staticConfigDict
    d['host.id'] = None
    stcfg = StaticConfig.fromDict(d)
    assert stcfg.hostId is not None


def test_static_member_meta(assignmentVersion):
    smm = StaticMemberMeta('hostId', 0, assignmentVersion)
    assert smm.assignmentVersion == assignmentVersion
    assert smm.hostId == 'hostId'
    assert smm.memberId == 0

    smm = StaticMemberMeta('hostId')
    assert smm.hostId == 'hostId'
    assert smm.assignmentVersion is None
    assert smm.memberId is None


def test_static_member_meta_asDict(assignmentVersion):
    smm = StaticMemberMeta('hostId', 0, assignmentVersion)
    smmd = smm.asDict()
    assert smmd['hostId'] == 'hostId'
    assert smmd['memberId'] == 0
    assert smmd['assignment']['configVersion'] == assignmentVersion.configVersion
    assert smmd['assignment']['version'] == assignmentVersion.version

    smm = StaticMemberMeta('hostId', 0)
    smmd = smm.asDict()
    assert smmd['hostId'] == 'hostId'
    assert smmd['memberId'] == 0
    assert smmd['assignment']['configVersion'] is None
    assert smmd['assignment']['version'] is None

    smm = StaticMemberMeta('hostId', 0, AssignmentVersion(None, None, 'group'))
    smmd = smm.asDict()
    assert smmd['hostId'] == 'hostId'
    assert smmd['memberId'] == 0
    assert smmd['assignment']['configVersion'] is None
    assert smmd['assignment']['version'] is None


def test_static_coordinator_raises_not_implemented(assignments, staticMemberMeta):
    smm = staticMemberMeta
    stcoord = StaticCoordinator()

    with pytest.raises(NotImplementedError):
        stcoord.assignments(smm)

    with pytest.raises(NotImplementedError):
        stcoord.heartbeat(smm)

    with pytest.raises(NotImplementedError):
        stcoord.join(smm)

    with pytest.raises(NotImplementedError):
        stcoord.leave(smm)

    with pytest.raises(NotImplementedError):
        stcoord.updateAssignments(smm, assignments)

    with pytest.raises(NotImplementedError):
        stcoord.start()

    with pytest.raises(NotImplementedError):
        stcoord.stop()


def test_static_membership_init_bad_args(mockedMembership):

    stconMock, stcoordMock, _ = mockedMembership

    with pytest.raises(ValueError):
        StaticMembership(None, stconMock, stcoordMock)

    with pytest.raises(ValueError):
        StaticMembership(staticConfig, None, stcoordMock)

    with pytest.raises(ValueError):
        StaticMembership(staticConfig, stconMock, None)


def test_static_membership_consumer_open_fails(mockedMembership):
    cons, coord, sm = mockedMembership
    cons.open.side_effect = Exception('boom')

    with pytest.raises(Exception):
        sm.step(True)

    assert sm.state() is None


def test_static_membership_coord_start_fails(mockedMembership):
    cons, coord, sm = mockedMembership
    coord.start.side_effect = Exception('boom')

    with pytest.raises(Exception):
        sm.step(True)

    assert sm.state() is None


@pytest.mark.parametrize(
    'desc, cfgVersion, asnCfgVersion, cfgTopics, asnTopics, callCount',
    [
        ('config and assignment are the same', 1, 1, {'t1': 10}, {'t1': 10}, 0),
        ('config version has been updated', 2, 1, {'t1': 10}, {'t1': 10}, 1),
        ('new topic has been added', 1, 1, {'t1': 10, 't2': 6}, {'t1': 10}, 1),
        ('topic is invalid config updated', 2, 1, {'t1': 0}, {'t1': 10}, 1),
        ('topic is invalid', 1, 1, {'t1': 0}, {'t1': 10}, 0),
        ('partitions have changed', 1, 1, {'t1': 12}, {'t1': 10}, 1),
        ('no assignments are avaliable', 1, None, {'t1': 10}, None, 1),
    ],
)
def test_static_membership_updates_assignments_initially(
    desc, cfgVersion, asnCfgVersion, cfgTopics, asnTopics, callCount, assignments, mockedMembership
):

    cons, coord, sm = mockedMembership
    if asnTopics is None:
        coord.assignments.return_value = None
    else:
        coord.assignments.return_value = assignments
        assignments.topics = asnTopics

    sm._conf.configVersion = cfgVersion
    assignments.configVersion = asnCfgVersion
    sm._conf.topics = cfgTopics.keys()
    cons.topicPartitionCount.side_effect = list(cfgTopics.values())
    # prevent going to join state
    coord.join.return_value = None

    state = sm.step(True)
    assert state == sm.STATE_NEW
    assert coord.updateAssignments.call_count == callCount


def test_static_membership_updates_assignments_initially_err(assignments, mockedMembership):
    cons, coord, sm = mockedMembership

    sm._conf.configVersion = assignments.configVersion
    sm._conf.topics = assignments.topics.keys()
    cons.topicPartitionCount.side_effect = list(assignments.topics.values())
    # prevent going to join state
    coord.join.return_value = None

    # when fetching assignments from zookeeper
    coord.assignments.side_effect = ValueError('boom1')
    with pytest.raises(ValueError) as e:
        sm.step(True)
    assert str(e.value) == 'boom1'
    coord.assignments.side_effect = None
    coord.assignments.return_value = assignments

    # when fetching topic metadata from the consumer
    cons.topicPartitionCount.side_effect = ValueError('boom2')
    with pytest.raises(ValueError) as e:
        sm.step(True)
    assert str(e.value) == 'boom2'
    cons.topicPartitionCount.side_effect = list(assignments.topics.values())

    # when updating assignments to zookeeper
    sm._conf.configVersion = assignments.configVersion + 1
    coord.updateAssignments.side_effect = ValueError('boom3')
    with pytest.raises(ValueError) as e:
        sm.step(True)
    assert str(e.value) == 'boom3'


def test_static_membership_join_with_memberid(mid, validMockedMembership):
    asn, cons, coord, sm = validMockedMembership

    coord.join.return_value = mid
    state = sm.step(True)
    assert state == sm.STATE_JOINED


def test_static_membership_no_join_without_memberid(validMockedMembership):
    asn, cons, coord, sm = validMockedMembership

    coord.join.return_value = None
    state = sm.step(True)
    assert state == sm.STATE_NEW


def test_static_membership_join_err(validMockedMembership):
    asn, cons, coord, sm = validMockedMembership

    coord.join.side_effect = ValueError('boom')
    state = sm.step(True)
    assert state == sm.STATE_STOPPING


def test_static_membership_join_then_memberid_none(mid, validMockedMembership):
    asn, cons, coord, sm = validMockedMembership

    test_static_membership_join_with_memberid(mid, validMockedMembership)
    coord.heartbeat.return_value = None
    state = sm.step()
    assert state == sm.STATE_NEW


def test_static_membership_join_then_memberid_different(validMockedMembership):
    asn, cons, coord, sm = validMockedMembership

    test_static_membership_join_with_memberid(0, validMockedMembership)
    coord.heartbeat.return_value = 1
    state = sm.step()
    assert state == sm.STATE_NEW


def test_static_membership_assigned(mid, validMockedMembership):
    asn, cons, coord, sm = validMockedMembership

    test_static_membership_join_with_memberid(mid, validMockedMembership)
    coord.heartbeat.return_value = mid
    state = sm.step()
    assert state == sm.STATE_ASSIGNED
    assert sm._memberAssignment is not None
    assert sm._memberAssignment == asn.getMemberAssignment(sm._memberId)


def test_static_membership_assigned_assignment_changed(assignments, validMockedMembership):
    asn, cons, coord, sm = validMockedMembership

    assert assignments.changeMaxMembers(4)

    test_static_membership_assigned(0, validMockedMembership)
    coord.assignments.return_value = assignments
    state = sm.step()
    assert state == sm.STATE_CONSUMING
    assert sm._memberAssignment is not None
    assert sm._memberAssignment == assignments.getMemberAssignment(0)
    assert cons.assign.call_count == 1


def test_static_membership_assigned_memberid_suddenly_invalid(validMockedMembership):
    asn, cons, coord, sm = validMockedMembership
    newAsn = Assignments(asn.group, 4, asn.topics, asn.configVersion, asn.version + 1)

    test_static_membership_assigned(5, validMockedMembership)

    coord.assignments.return_value = newAsn
    state = sm.step()
    assert state == sm.STATE_NEW


def test_static_membership_consuming_topics_change(validMockedMembership):
    pass


def test_static_membership_consuming_consumer_err(validMockedMembership):
    pass


def test_static_membership_consuming_hearbeat_err(validMockedMembership):
    pass


@pytest.mark.parametrize('hbReturn', [None, 5, 100])
def test_static_membership_assigned_then_lost(hbReturn, validMockedMembership):
    asn, cons, coord, sm = validMockedMembership

    test_static_membership_join_with_memberid(0, validMockedMembership)
    coord.heartbeat.return_value = None
    state = sm.step()
    assert state == sm.STATE_NEW


def test_static_membership_shutdown_not_started(mockedMembership):
    cons, coord, sm = mockedMembership

    try:
        sm.stop()
    except Exception:
        pytest.fail('Exception not expected')

    assert cons.close.call_count == 1
    assert coord.stop.call_count == 1
    assert coord.leave.call_count == 1
    assert sm.state() is None
