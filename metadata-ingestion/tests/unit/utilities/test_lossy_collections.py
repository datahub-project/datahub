import random
import re
import time

import pytest

from datahub.utilities.lossy_collections import LossyDict, LossyList, LossySet


@pytest.mark.parametrize("length, sampling", [(10, False), (100, True)])
def test_lossylist_sampling(length, sampling):
    l_dict: LossyList[str] = LossyList()
    for i in range(0, length):
        l_dict.append(f"{i} Hello World")

    assert len(l_dict) == length
    assert l_dict.sampled is sampling
    if sampling:
        assert f"... sampled of {length} total elements" in str(l_dict)
    else:
        assert "sampled" not in str(l_dict)

    list_version = [int(i.split(" ")[0]) for i in l_dict]
    print(list_version)
    assert sorted(list_version) == list_version


@pytest.mark.parametrize("length, sampling", [(10, False), (100, True)])
def test_lossyset_sampling(length, sampling):
    lossy_set: LossySet[str] = LossySet()
    for i in range(0, length):
        lossy_set.add(f"{i} Hello World")

    assert len(lossy_set) == min(10, length)
    assert lossy_set.sampled is sampling
    if sampling:
        assert f"... sampled with at most {length - 10} elements missing" in str(
            lossy_set
        )
    else:
        assert "sampled" not in str(lossy_set)

    list_version = [int(i.split(" ")[0]) for i in lossy_set]
    set_version = set(list_version)

    assert len(list_version) == len(set_version)
    assert len(list_version) == min(10, length)


@pytest.mark.parametrize(
    "length, sampling, sub_length", [(4, False, 4), (10, False, 14), (100, True, 1000)]
)
def test_lossydict_sampling(length, sampling, sub_length):
    lossy_dict: LossyDict[int, LossyList[str]] = LossyDict()
    elements_added = 0
    element_length_map = {}
    for i in range(0, length):
        list_length = random.choice(range(1, sub_length))
        element_length_map[i] = 0
        for _num_elements in range(0, list_length):
            if not lossy_dict.get(i):
                elements_added += 1
                # reset to 0 until we get it back
                element_length_map[i] = 0
            else:
                element_length_map[i] = len(lossy_dict[i])

            current_list = lossy_dict.get(i, LossyList())
            current_list.append(f"{i}:{round(time.time(), 2)} Hello World")
            lossy_dict[i] = current_list
            element_length_map[i] += 1

    assert len(lossy_dict) == min(lossy_dict.max_elements, length)
    assert lossy_dict.sampled is sampling
    if sampling:
        assert re.search("sampled of at most .* entries.", str(lossy_dict))
        assert (
            f"{lossy_dict.max_elements} sampled of at most {elements_added} entries."
            in str(lossy_dict)
        )
    else:
        # cheap way to determine that the dict isn't reporting sampled keys
        assert not re.search("sampled of at most .* entries.", str(lossy_dict))

    for k, v in lossy_dict.items():
        assert len(v) == element_length_map[k]
