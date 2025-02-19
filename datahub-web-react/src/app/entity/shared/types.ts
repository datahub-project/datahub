import { MutationFunctionOptions, FetchResult } from '@apollo/client';
import React from 'react';

import {
    DataPlatform,
    DatasetEditableProperties,
    DatasetEditablePropertiesUpdate,
    RawAspect,
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
    EntityLineageResult,
    SubTypes,
    Container,
    Health,
    Status,
    Deprecation,
    DataPlatformInstance,
    ParentContainersResult,
    EntityRelationshipsResult,
    ParentNodesResult,
    SiblingProperties,
    CustomPropertiesEntry,
    DomainAssociation,
    InputFields,
    FineGrainedLineage,
    EntityPrivileges,
    Embed,
    FabricType,
    BrowsePathV2,
    DataJobInputOutput,
    ParentDomainsResult,
    StructuredProperties,
    Forms,
    ScrollResults,
    Documentation,
    DisplayProperties,
    VersionProperties,
    DataProcessRunEvent,
} from '../../../types.generated';
import { FetchedEntity } from '../../lineage/types';

export type EntityTab = {
    name: string;
    component: React.FunctionComponent<{ properties?: any }>;
    display?: {
        visible: (GenericEntityProperties, T) => boolean; // Whether the tab is visible on the UI. Defaults to true.
        enabled: (GenericEntityProperties, T) => boolean; // Whether the tab is enabled on the UI. Defaults to true.
    };
    properties?: any;
    id?: string;
    getDynamicName?: (GenericEntityProperties, T) => string;
};

export type EntitySidebarSection = {
    component: React.FunctionComponent<{ properties?: any; readOnly?: boolean }>;
    display?: {
        visible: (GenericEntityProperties, T) => boolean; // Whether the sidebar is visible on the UI. Defaults to true.
    };
    properties?: any;
};

export type EntitySubHeaderSection = {
    component: React.FunctionComponent<{ properties?: any }>;
};

export type GenericEntityProperties = {
    urn?: string;
    type?: EntityType;
    name?: Maybe<string>;
    properties?: Maybe<{
        name?: Maybe<string>;
        description?: Maybe<string>;
        qualifiedName?: Maybe<string>;
        sourceUrl?: Maybe<string>;
        sourceRef?: Maybe<string>;
        businessAttributeDataType?: Maybe<string>;
        externalUrl?: Maybe<string>;
    }>;
    globalTags?: Maybe<GlobalTags>;
    glossaryTerms?: Maybe<GlossaryTerms>;
    ownership?: Maybe<Ownership>;
    domain?: Maybe<DomainAssociation>;
    dataProduct?: Maybe<EntityRelationshipsResult>;
    platform?: Maybe<DataPlatform>;
    dataPlatformInstance?: Maybe<DataPlatformInstance>;
    customProperties?: Maybe<CustomPropertiesEntry[]>;
    structuredProperties?: Maybe<StructuredProperties>;
    institutionalMemory?: Maybe<InstitutionalMemory>;
    schemaMetadata?: Maybe<SchemaMetadata>;
    externalUrl?: Maybe<string>;
    entityTypeOverride?: Maybe<string>; // to indicate something is a Stream, View instead of Dataset... etc
    /** Dataset specific- TODO, migrate these out */
    editableSchemaMetadata?: Maybe<EditableSchemaMetadata>;
    editableProperties?: Maybe<DatasetEditableProperties>;
    autoRenderAspects?: Maybe<Array<RawAspect>>;
    lineageUrn?: string; // If set, render this urn's lineage instead if not in separate siblings mode
    lineageSiblingIcon?: string; // If set, render this entity in lineage along with the sibling icon and do not separate siblings in the sidebar
    upstream?: Maybe<EntityLineageResult>;
    downstream?: Maybe<EntityLineageResult>;
    subTypes?: Maybe<SubTypes>;
    entityCount?: number;
    container?: Maybe<Container>;
    health?: Maybe<Array<Health>>;
    status?: Maybe<Status>;
    deprecation?: Maybe<Deprecation>;
    siblings?: Maybe<SiblingProperties>;
    siblingsSearch?: Maybe<ScrollResults>;
    parentContainers?: Maybe<ParentContainersResult>;
    parentDomains?: Maybe<ParentDomainsResult>;
    children?: Maybe<EntityRelationshipsResult>;
    parentNodes?: Maybe<ParentNodesResult>;
    isAChildren?: Maybe<EntityRelationshipsResult>;
    siblingPlatforms?: Maybe<DataPlatform[]>;
    lastIngested?: Maybe<number>;
    inputFields?: Maybe<InputFields>;
    fineGrainedLineages?: Maybe<FineGrainedLineage[]>;
    privileges?: Maybe<EntityPrivileges>;
    embed?: Maybe<Embed>;
    exists?: boolean;
    origin?: Maybe<FabricType>;
    documentation?: Maybe<Documentation>;
    browsePathV2?: Maybe<BrowsePathV2>;
    inputOutput?: Maybe<DataJobInputOutput>;
    forms?: Maybe<Forms>;
    parent?: Maybe<GenericEntityProperties>;
    displayProperties?: Maybe<DisplayProperties>;
    notes?: Maybe<EntityRelationshipsResult>;
    versionProperties?: Maybe<VersionProperties>;

    // Data process instance
    lastRunEvent?: Maybe<DataProcessRunEvent>;
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

interface EntityState {
    shouldRefetchContents: boolean;
    setShouldRefetchContents: (shouldRefetch: boolean) => void;
}

export enum DrawerType {
    VERSIONS,
}

export type EntityContextType = {
    urn: string;
    entityType: EntityType;
    dataNotCombinedWithSiblings: any;
    entityData: GenericEntityProperties | null;
    loading: boolean;
    baseEntity: any;
    updateEntity?: UpdateEntityType<any> | null;
    routeToTab: (params: { tabName: string; tabParams?: Record<string, any>; method?: 'push' | 'replace' }) => void;
    refetch: () => Promise<any>;
    lineage?: FetchedEntity | undefined;
    shouldRefetchEmbeddedListSearch?: boolean;
    setShouldRefetchEmbeddedListSearch?: React.Dispatch<React.SetStateAction<boolean>>;
    entityState?: EntityState;
    setDrawer?: React.Dispatch<React.SetStateAction<DrawerType | undefined>>;
};

export type SchemaContextType = {
    refetch?: () => Promise<any>;
};

export type RequiredAndNotNull<T> = {
    [P in keyof T]-?: Exclude<T[P], null | undefined>;
};

export type EntityAndType = {
    urn: string;
    type: EntityType;
};
