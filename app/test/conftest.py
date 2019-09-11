import pytest

from app.static_assignment.assignments import Assignments
from app.static_assignment.assignments import AssignmentVersion
from app.static_assignment.assignments import MemberAssignment
from app.static_assignment.group import StaticMemberMeta

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


@pytest.fixture()
def mbrAssignment():
    mbr = MemberAssignment(1)
    mbr.assign('t1', 0)
    mbr.assign('t1', 1)
    mbr.assign('t2', 0)
    mbr.assign('t2', 1)

    return mbr


@pytest.fixture()
def assignments():
    return Assignments.fromJson(good_assignment_json)


@pytest.fixture()
def assignmentVersion():
    return AssignmentVersion(5, 2, 'group')


@pytest.fixture()
def staticMemberMeta(assignmentVersion):
    return StaticMemberMeta('hostId', 8, assignmentVersion)
