import {
    Chart,
    Dashboard,
    DataFlow,
    DataJob,
    Dataset,
    Entity,
    EntityType,
    GlobalTags,
    GlobalTagsUpdate,
    InstitutionalMemory,
    InstitutionalMemoryMetadata,
    InstitutionalMemoryUpdate,
    Owner,
    OwnerUpdate,
    TagAssociation,
} from '../types.generated';
import { findUserByURN } from './fixtures/searchResult/userSearchResult';
import { tagDb } from './fixtures/tag';
import { getActor } from './helper';

type UpdateEntityOwnersArg = {
    entity?: Entity;
    owners?: OwnerUpdate[];
};

export const updateEntityOwners = ({ entity, owners }: UpdateEntityOwnersArg) => {
    const updateOwners = owners
        ?.map((o) => {
            const user = findUserByURN(o.owner);
            return user
                ? {
                      owner: user,
                      type: o.type,
                      __typename: 'Owner',
                  }
                : null;
        })
        .filter(Boolean) as Owner[];

    const dataEntity = entity as Dataset | Chart | Dashboard | DataFlow | DataJob;
    if (dataEntity?.ownership?.owners) {
        // eslint-disable-next-line no-param-reassign
        dataEntity.ownership.owners = updateOwners;
    }
};

type UpdateEntityTagArg = {
    entity?: Entity;
    globalTags: GlobalTagsUpdate;
};

export const updateEntityTag = ({ entity, globalTags }: UpdateEntityTagArg) => {
    const tagAssociations = globalTags.tags
        ?.map((t) => {
            const tag = tagDb.find((ti) => {
                return ti.urn === t.tag.urn;
            });

            return tag
                ? {
                      tag,
                      __typename: 'TagAssociation',
                  }
                : null;
        })
        .filter(Boolean) as TagAssociation[];
    const baseTags: TagAssociation[] = [];
    const baseGlobalTags: GlobalTags = {
        __typename: 'GlobalTags',
        tags: baseTags,
    };
    const dataEntity = entity as Dataset | Chart | Dashboard | DataFlow | DataJob;

    dataEntity.globalTags = dataEntity.globalTags || baseGlobalTags;
    if (dataEntity.globalTags.tags) {
        dataEntity.globalTags.tags = tagAssociations;
    }
};

type UpdateEntityLinkArg = {
    entity: Dataset;
    institutionalMemory: InstitutionalMemoryUpdate;
};

export const updateEntityLink = ({ entity, institutionalMemory }: UpdateEntityLinkArg) => {
    const dataEntity = entity;
    const baseElements: InstitutionalMemoryMetadata[] = [];
    const baseInstitutionalMemory: InstitutionalMemory = {
        elements: baseElements,
        __typename: 'InstitutionalMemory',
    };

    dataEntity.institutionalMemory = dataEntity.institutionalMemory || baseInstitutionalMemory;
    dataEntity.institutionalMemory.elements = institutionalMemory.elements.map((e) => {
        const link: InstitutionalMemoryMetadata = {
            __typename: 'InstitutionalMemoryMetadata',
            url: e.url,
            description: e.description as string,
            label: e.description as string,
            author: { urn: e.author, username: '', type: EntityType.CorpUser },
            actor: { urn: e.author, username: '', type: EntityType.CorpUser },
            created: { time: Date.now(), actor: getActor(), __typename: 'AuditStamp' },
            associatedUrn: dataEntity.urn,
        };
        return link;
    });
};
