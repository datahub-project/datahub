from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel

from powerbireportserver.constants import RelationshipDirection


class CorpUserEditableInfo(BaseModel):
    displayName: str
    title: str
    aboutMe: Optional[str]
    teams: Optional[List[str]]
    skills: Optional[List[str]]
    pictureLink: Optional[str]


class CorpUserEditableProperties(CorpUserEditableInfo):
    slack: Optional[str]
    phone: Optional[str]
    email: str


class CorpUserStatus(BaseModel):
    active: bool


class GlobalTags(BaseModel):
    tags: List[str]


class EntityRelationship(BaseModel):
    type: str
    direction: RelationshipDirection
    entity: str
    created: datetime


class EntityRelationshipsResult(BaseModel):
    start: int
    count: int
    total: int
    relationships: Optional[EntityRelationship]


class CorpUserProperties(BaseModel):
    active: bool
    displayName: str
    email: str
    title: Optional[str]
    manager: Optional["CorpUser"]
    departmentId: Optional[int]
    departmentName: Optional[str]
    firstName: Optional[str]
    lastName: Optional[str]
    fullName: Optional[str]
    countryCode: Optional[str]


class CorpUserInfo(CorpUserProperties):
    """Corp User Info"""


class CorpUser(BaseModel):
    urn: str
    type: str
    username: str
    properties: CorpUserProperties
    editableProperties: Optional[CorpUserEditableProperties]
    status: Optional[CorpUserStatus]
    tags: Optional[GlobalTags]
    relationships: Optional[EntityRelationshipsResult]
    info: Optional[CorpUserInfo]
    editableInfo: Optional[CorpUserEditableInfo]
    globalTags: Optional[GlobalTags]

    def get_urn_part(self):
        return "{}".format(self.username)

    def __members(self):
        return (self.username,)

    def __eq__(self, instance):
        return (
            isinstance(instance, CorpUser) and self.__members() == instance.__members()
        )

    def __hash__(self):
        return hash(self.__members())


CorpUserProperties.update_forward_refs()
