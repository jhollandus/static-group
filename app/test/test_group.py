import pytest

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
def staticConfig(staticConfigDict):
    return StaticConfig.fromDict(staticConfigDict)


@pytest.fixture()
def mockedMembership(mocker, staticConfig):
    mockCoord = mocker.MagicMock()
    mockCons = mocker.MagicMock()
    sm = StaticMembership(staticConfig, mockCons, mockCoord)

    return (mockCons, mockCoord, sm)


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
    assert stcfg.hostId == d['hostId']
    assert stcfg.topics == d['topics']
    assert stcfg.group == d['group']
    assert stcfg.maxGroupSize == d['maxGroupSize']
    assert stcfg.configVersion == d['configVersion']
    assert stcfg.zkConnect == d['zkConnect']
    assert stcfg.kafkaConnect == d['kafkaConnect']
    assert stcfg.more == d['more']


def test_static_config_from_no_hostId(staticConfigDict):
    d = staticConfigDict
    d['hostId'] = None
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


def test_static_membership_updates_assignments(assignments, mockedMembership):
    cons, coord, sm = mockedMembership
    sm._conf.topics = assignments.topics.keys()
    sm._conf.configVersion = assignments.configVersion

    cons.topicPartitionCount.return_value = list(assignments.topics.values())[0]
    coord.assignments.return_value = assignments
    coord.join.return_value = None

    state = sm.step(True)
    assert state == sm.STATE_NEW
    coord.updateAssignments.assert_not_called()

    # now change the config version
    sm._conf.configVersion = assignments.configVersion + 1
    state = sm.step(True)
    assert state == sm.STATE_NEW
    coord.updateAssignments.assert_called_once()

    # revert the config version
    sm._conf.configVersion = assignments.configVersion

    # now return None for the assignments
    coord.assignments.return_value = None
    state = sm.step(True)
    assert state == sm.STATE_NEW
    assert coord.updateAssignments.call_count == 2
