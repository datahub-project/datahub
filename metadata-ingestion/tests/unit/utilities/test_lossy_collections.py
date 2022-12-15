import random
import re
import time

import pytest

from datahub.utilities.lossy_collections import LossyDict, LossyList, LossySet


@pytest.mark.parametrize("length, sampling", [(10, False), (100, True)])
def test_lossylist_sampling(length, sampling):
    l: LossyList[str] = LossyList()
    for i in range(0, length):
        l.append(f"{i} Hello World")

    assert len(l) == length
    assert l.sampled is sampling
    if sampling:
        assert f"... sampled of {length} total elements" in str(l)
    else:
        assert "sampled" not in str(l)

    list_version = [int(i.split(" ")[0]) for i in l]
    print(list_version)
    assert sorted(list_version) == list_version


@pytest.mark.parametrize("length, sampling", [(10, False), (100, True)])
def test_lossyset_sampling(length, sampling):
    l: LossySet[str] = LossySet()
    for i in range(0, length):
        l.add(f"{i} Hello World")

    assert len(l) == min(10, length)
    assert l.sampled is sampling
    if sampling:
        assert f"... sampled with at most {length-10} elements missing" in str(l)
    else:
        assert "sampled" not in str(l)

    list_version = [int(i.split(" ")[0]) for i in l]
    set_version = set(list_version)

    assert len(list_version) == len(set_version)
    assert len(list_version) == min(10, length)


@pytest.mark.parametrize(
    "length, sampling, sub_length", [(4, False, 4), (10, False, 14), (100, True, 1000)]
)
def test_lossydict_sampling(length, sampling, sub_length):
    l: LossyDict[int, LossyList[str]] = LossyDict()
    elements_added = 0
    element_length_map = {}
    for i in range(0, length):
        list_length = random.choice(range(1, sub_length))
        element_length_map[i] = 0
        for num_elements in range(0, list_length):
            if not l.get(i):
                elements_added += 1
                # reset to 0 until we get it back
                element_length_map[i] = 0
            else:
                element_length_map[i] = len(l[i])

            current_list = l.get(i, LossyList())
            current_list.append(f"{i}:{round(time.time(),2)} Hello World")
            l[i] = current_list
            element_length_map[i] += 1

    assert len(l) == min(l.max_elements, length)
    assert l.sampled is sampling
    if sampling:
        assert re.search("sampled of at most .* entries.", str(l))
        assert f"{l.max_elements} sampled of at most {elements_added} entries." in str(
            l
        )
    else:
        # cheap way to determine that the dict isn't reporting sampled keys
        assert not re.search("sampled of at most .* entries.", str(l))

    for k, v in l.items():
        assert len(v) == element_length_map[k]
