import pytest
from static_assignment.static_group import AssignmentCalculator
from static_assignment.static_group import Assignments

good_assignment_json = '''
    {
        "group":"test",
        "topics":{"locations":12},
        "maxMembers":6,
        "version":1,
        "configVersion":12345,
        "memberAssignments":[
            {"memberId":0,"topics":{"locations":[0,1]}},
            {"memberId":1,"topics":{"locations":[2,3]}},
            {"memberId":2,"topics":{"locations":[4,5]}},
            {"memberId":3,"topics":{"locations":[6,7]}},
            {"memberId":4,"topics":{"locations":[8,9]}},
            {"memberId":5,"topics":{"locations":[10,11]}}
        ]
    }
'''

tp1 = {'locations': 12}
tp2 = {'locations': 12, 'users': 4}
tp3 = {'locations': 12, 'users': 4, 'drivers': 3}


@pytest.mark.parametrize('size,partitions', [(0, 12), (0, 0)])
def test_assignment_calculator_zero_group_size(size, partitions):
    calc = AssignmentCalculator(size, {'locations': partitions})
    mbrs = calc.generateAssignments()
    assert len(mbrs) == 0


def test_assignment_calculator_total_partitions():
    calc = AssignmentCalculator(1, tp3)
    assert calc._totalPartitions() == 19


def test_assignment_calculator_iterator():
    calc = AssignmentCalculator(1, {'t1': 2, 't2': 3, 't3': 1})
    rslts = list(calc._partitionsByIndex())
    assert rslts == [('t1', 0), ('t2', 0), ('t3', 0), ('t1', 1), ('t2', 1), ('t2', 2)]


@pytest.mark.parametrize('groupSize,topicPartitions', [(1, tp1), (12, tp1), (13, tp1), (1, tp3), (20, tp3), (8, tp2)])
def test_assignment_calculator_distribution(groupSize, topicPartitions):
    calc = AssignmentCalculator(groupSize, topicPartitions)
    mbrs = calc.generateAssignments()

    # verify number of members
    assert len(mbrs) == groupSize

    found = {}
    maxCount = 0
    minCount = 999999999

    for m in mbrs:
        print(m)
        for t, ps in m.topics.items():
            assert t in topicPartitions

            pl = m.totalAssignments()
            if pl > maxCount:
                maxCount = pl
            if pl < minCount:
                minCount = pl

            for p in ps:
                assert p < topicPartitions[t]
                key = f'{t}-{p}'
                fc = found.get(key, 0)
                found[key] = fc + 1

    # verify even distribution
    assert abs(maxCount - minCount) < 2

    totalPartCount = sum(topicPartitions.values())

    # verify each assignment is used just once
    assert sum(found.values()) == totalPartCount


def test_assignment_json_serde():
    asns = Assignments('test', 8, tp1, 12345, 1)
    jsonOut = asns.asJson()
    print(jsonOut)
    asnsDeser = Assignments.fromJson(jsonOut)

    assert asns.version == asnsDeser.version
    assert asns.group == asnsDeser.group
    assert asns.maxMembers == asnsDeser.maxMembers
    assert asns.configVersion == asnsDeser.configVersion
    assert len(asns.memberAssignments) == len(asnsDeser.memberAssignments)
    assert len(asns.topics) == len(asnsDeser.topics)


def test_assignment_json_bad():
    rs = Assignments.fromJson('{some: bad json}')
    assert rs is None


@pytest.mark.parametrize(
    'jsonStr',
    [
        '{"maxMembers": 2, "group": "foo", "configVersion": 123, "version": 1, "memberAssignments":[]}',  # missing 'topics'
        '{"group":"test","topics":{"locations":12},"maxMembers":8,"version":"foo","configVersion":12345,"memberAssignments":[]}',  # version is a str
        '{"group":"test","topics":[],"maxMembers":8,"version": 1,"configVersion":12345,"memberAssignments":[]}',  # topics is an array
    ],
)
def test_assignment_json_bad_input(jsonStr):
    rs = Assignments.fromJson(jsonStr)
    assert rs is None


def test_assignment_version():
    asns = Assignments.fromJson(good_assignment_json)
    av = asns.assignmentVersion()
    assert av.configVersion == asns.configVersion
    assert av.group == asns.group
    assert av.version == asns.version


def test_assignment_change_size():
    asns = Assignments.fromJson(good_assignment_json)

    with pytest.raises(ValueError):
        changed = asns.changeMaxMembers(0)

    changed = asns.changeMaxMembers(8)
    assert changed

    changed = asns.changeMaxMembers(8)
    assert not changed


def test_assignment_change_topics():
    asns = Assignments.fromJson(good_assignment_json)

    changed = asns.changeTopicPartitions(tp2)
    assert changed

    changed = asns.changeTopicPartitions(tp2)
    assert not changed
