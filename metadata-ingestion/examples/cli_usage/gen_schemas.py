import json
from typing import List, Union

from pydantic import BaseModel

from datahub.api.entities.corpgroup.corpgroup import CorpGroup
from datahub.api.entities.corpuser.corpuser import CorpUser

"""
A handy script to generate schemas for the CorpUser and CorpGroup file formats
"""


class CorpUserList(BaseModel):
    __root__: List[CorpUser]


class CorpGroupList(BaseModel):
    __root__: List[CorpGroup]


class CorpUserFile(BaseModel):
    __root__: Union[CorpUser, CorpUserList]


class CorpGroupFile(BaseModel):
    __root__: Union[CorpGroup, CorpGroupList]


with open("user/user.dhub.yaml_schema.json", "w") as fp:
    fp.write(json.dumps(CorpUserFile.schema(), indent=4))

with open("group/group.dhub.yaml_schema.json", "w") as fp:
    fp.write(json.dumps(CorpGroupFile.schema(), indent=4))
