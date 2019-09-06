import pytest
from static_assignment.static_group import AssignmentCalculator

@pytest.mark.parametrize("groupSize,partitions", [(0, 12), (0, 0)])
def test_assignment_calculator_zero_group_size(groupSize, partitions):
    calc = AssignmentCalculator(groupSize, {'locations': partitions})
    mbrs = calc.generateAssignments()
    assert len(mbrs) == 0

tp1 = {'locations': 12}
tp2 = {'locations': 12, 'users': 4}
tp3 = {'locations': 12, 'users': 4, 'drivers': 3}
@pytest.mark.parametrize("groupSize,topicPartitions", [(1, tp1), (12, tp1), (13, tp1), (1, tp3), (20, tp3), (8, tp2)])
def test_assignment_calculator_distribution(groupSize, topicPartitions):
    calc = AssignmentCalculator(groupSize, topicPartitions)
    mbrs = calc.generateAssignments()

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
                key = f"{t}-{p}"
                fc = found.get(key, 0)
                found[key] = fc + 1
    assert abs(maxCount - minCount) < 2
    totalPartCount = 0
    for t, p in topicPartitions.items():
        totalPartCount += p
    print(found)
    assert len(found) == totalPartCount
            