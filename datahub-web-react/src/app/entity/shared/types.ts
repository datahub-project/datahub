import { MutationFunctionOptions, FetchResult } from '@apollo/client';

import {
    DataPlatform,
    DatasetEditableProperties,
    DatasetEditablePropertiesUpdate,
    DownstreamEntityRelationships,
    EditableSchemaMetadata,
    EditableSchemaMetadataUpdate,
    EntityType,
    GlobalTags,
    GlobalTagsUpdate,
    GlossaryTerms,
    InstitutionalMemory,
    InstitutionalMemoryUpdate,
    Maybe,
    Ownership,
    OwnershipUpdate,
    SchemaMetadata,
    StringMapEntry,
    UpstreamEntityRelationships,
} from '../../../types.generated';
import { FetchedEntity } from '../../lineage/types';

export type EntityTab = {
    name: string;
    component: React.FunctionComponent;
    shouldHide?: (GenericEntityProperties, T) => boolean;
};

export type EntitySidebarSection = {
    component: React.FunctionComponent<{ properties?: any }>;
    shouldHide?: (GenericEntityProperties, T) => boolean;
    properties?: any;
};

export type GenericEntityProperties = {
    urn?: string;
    name?: Maybe<string>;
    description?: Maybe<string>;
    globalTags?: Maybe<GlobalTags>;
    glossaryTerms?: Maybe<GlossaryTerms>;
    ownership?: Maybe<Ownership>;
    platform?: Maybe<DataPlatform>;
    customProperties?: Maybe<StringMapEntry[]>;
    institutionalMemory?: Maybe<InstitutionalMemory>;
    schemaMetadata?: Maybe<SchemaMetadata>;
    externalUrl?: Maybe<string>;
    // to indicate something is a Stream, View instead of Dataset... etc
    entityTypeOverride?: Maybe<string>;
    /** Dataset specific- TODO, migrate these out */
    editableSchemaMetadata?: Maybe<EditableSchemaMetadata>;
    editableProperties?: Maybe<DatasetEditableProperties>;
};

export type GenericEntityUpdate = {
    editableProperties?: Maybe<DatasetEditablePropertiesUpdate>;
    globalTags?: Maybe<GlobalTagsUpdate>;
    ownership?: Maybe<OwnershipUpdate>;
    institutionalMemory?: Maybe<InstitutionalMemoryUpdate>;
    /** Dataset specific- TODO, migrate these out */
    editableSchemaMetadata?: Maybe<EditableSchemaMetadataUpdate>;
};

export type UpdateEntityType<U> = (
    options?:
        | MutationFunctionOptions<
              U,
              {
                  urn: string;
                  input: GenericEntityUpdate;
              }
          >
        | undefined,
) => Promise<FetchResult<U, Record<string, any>, Record<string, any>>>;

export type EntityContextType = {
    urn: string;
    entityType: EntityType;
    entityData: GenericEntityProperties | null;
    baseEntity: any;
    updateEntity: UpdateEntityType<any>;
    routeToTab: (params: { tabName: string; tabParams?: Record<string, any>; method?: 'push' | 'replace' }) => void;
    refetch: () => Promise<any>;
    lineageMetadata: FetchedEntity | undefined;
};

export type RequiredAndNotNull<T> = {
    [P in keyof T]-?: Exclude<T[P], null | undefined>;
};
