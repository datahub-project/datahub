import json
from typing import List, Union

from pydantic import RootModel

from datahub.api.entities.corpgroup.corpgroup import CorpGroup
from datahub.api.entities.corpuser.corpuser import CorpUser

"""
A handy script to generate schemas for the CorpUser and CorpGroup file formats.

Run this from the examples/cli_usage/ directory -- the output paths are relative.
"""


class CorpUserList(RootModel[List[CorpUser]]):
    pass


class CorpGroupList(RootModel[List[CorpGroup]]):
    pass


class CorpUserFile(RootModel[Union[CorpUser, CorpUserList]]):
    pass


class CorpGroupFile(RootModel[Union[CorpGroup, CorpGroupList]]):
    pass


with open("user/user.dhub.yaml_schema.json", "w") as fp:
    fp.write(json.dumps(CorpUserFile.model_json_schema(), indent=4))

with open("group/group.dhub.yaml_schema.json", "w") as fp:
    fp.write(json.dumps(CorpGroupFile.model_json_schema(), indent=4))
