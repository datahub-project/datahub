from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

from .constants import RelationshipDirection


class CorpUserEditableInfo(BaseModel):
    display_name: str = Field(alias="displayName")
    title: str
    about_me: Optional[str] = Field(alias="aboutMe")
    teams: Optional[List[str]]
    skills: Optional[List[str]]
    picture_link: Optional[str] = Field(alias="pictureLink")


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
    display_name: str = Field(alias="displayName")
    email: str
    title: Optional[str]
    manager: Optional["CorpUser"]
    department_id: Optional[int] = Field(alias="departmentId")
    department_name: Optional[str] = Field(alias="departmentName")
    first_name: Optional[str] = Field(alias="firstName")
    last_name: Optional[str] = Field(alias="lastName")
    full_name: Optional[str] = Field(alias="fullName")
    country_code: Optional[str] = Field(alias="countryCode")


class CorpUser(BaseModel):
    urn: str
    type: str
    username: str
    properties: CorpUserProperties
    editable_properties: Optional[CorpUserEditableProperties] = Field(
        alias="editableProperties"
    )
    status: Optional[CorpUserStatus]
    tags: Optional[GlobalTags]
    relationships: Optional[EntityRelationshipsResult]
    editableInfo: Optional[CorpUserEditableInfo] = Field(alias="editableInfo")
    global_tags: Optional[GlobalTags] = Field(alias="globalTags")

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
