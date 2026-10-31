import { FetchResult, MutationFunctionOptions } from '@apollo/client';
import React from 'react';

import { FetchedEntity } from '@app/lineageV3/types';

import {
    ApplicationAssociation,
    AssetSettings,
    BrowsePathV2,
    Container,
    CustomPropertiesEntry,
    DataJobInputOutput,
    DataPlatform,
    DataPlatformInstance,
    DataProcessInstance,
    DataProcessRunEvent,
    DatasetEditableProperties,
    DatasetEditablePropertiesUpdate,
    Deprecation,
    DisplayProperties,
    Documentation,
    DomainAssociation,
    EditableSchemaMetadata,
    EditableSchemaMetadataUpdate,
    Embed,
    Entity,
    EntityLineageResult,
    EntityPrivileges,
    EntityRelationshipsResult,
    EntityType,
    FabricType,
    FineGrainedLineage,
    Forms,
    GlobalTags,
    GlobalTagsUpdate,
    GlossaryTerms,
    Health,
    InputFields,
    InstitutionalMemory,
    InstitutionalMemoryUpdate,
    Maybe,
    Ownership,
    OwnershipUpdate,
    ParentContainersResult,
    ParentDomainsResult,
    ParentNodesResult,
    RawAspect,
    ResolvedAuditStamp,
    SchemaMetadata,
    ScrollResults,
    SiblingProperties,
    Status,
    StructuredProperties,
    SubTypes,
    VersionProperties,
} from '@types';

export enum TabRenderType {
    /**
     * A default, full screen tab.
     */
    DEFAULT,
    /**
     * A compact tab
     */
    COMPACT,
}

export enum TabContextType {
    /**
     * A tab rendered horizontally in the main content.
     */
    PROFILE,
    /**
     * A tab rendered in the main profile sidebar.
     */
    PROFILE_SIDEBAR,
    /**
     * A tab rendered in the form sidebar.
     */
    FORM_SIDEBAR,
    /**
     * A tab rendered in the lineage sidebar
     */
    LINEAGE_SIDEBAR,
    /**
     * A tab rendered in the chrome extension sidebar
     */
    CHROME_SIDEBAR,
    /**
     * A tab rendered in the search sidebar
     */
    SEARCH_SIDEBAR,
}

export type EntityTabProps = {
    /**
     * The render type for the tab, e.g. whether it's full screen / horizontal or compact / vertical
     */
    renderType: TabRenderType;
    /**
     * The context type, detailing the scenario in which the tab is being rendered.
     */
    contextType: TabContextType;
    /**
     * Atr that can be provided from the outside.
     */
    properties?: any;
};

export type EntityTab = {
    name: string; // Display label; may be translated, so never use it to identify the tab.
    path: string; // Stable, language-independent URL segment that identifies the tab.
    component: React.FunctionComponent<EntityTabProps>;
    icon?: React.FunctionComponent<any>;
    display?: {
        visible: (GenericEntityProperties, T) => boolean; // Whether the tab is visible on the UI. Defaults to true.
        enabled: (GenericEntityProperties, T) => boolean; // Whether the tab is enabled on the UI. Defaults to true.
    };
    properties?: any;
    id?: string;
    getCount?: (GenericEntityProperties, T, loading: boolean) => number | undefined;
    supportsFullsize?: boolean; // As per TabFullsizedContext
};

export type EntitySidebarTab = {
    key?: string;
    name: string;
    component: React.FunctionComponent<EntityTabProps>;
    icon: React.FunctionComponent<any>;
    selectedIcon?: React.FunctionComponent<any>; // Icon to use when this tab is selected
    display?: {
        visible: (GenericEntityProperties, T) => boolean; // Whether the tab is visible on the UI. Defaults to true.
        enabled: (GenericEntityProperties, T) => boolean; // Whether the tab is enabled on the UI. Defaults to true.
    };
    description?: string; // Used to power tooltip if present.
    properties?: any;
    id?: string;
};

export type EntitySidebarSection = {
    component: React.FunctionComponent<{
        properties?: any;
        readOnly?: boolean;
        renderType?: TabRenderType;
        contexType?: TabContextType;
    }>;
    display?: {
        visible: (GenericEntityProperties, T, contextType?: TabContextType | undefined) => boolean; // Whether the sidebar is visible on the UI. Defaults to true.
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
    // Deprecated in the schema in favour of `properties.description`, but still the only
    // description some types (notably Tag) expose and select in their fragments.
    description?: Maybe<string>;
    properties?: Maybe<{
        name?: Maybe<string>;
        description?: Maybe<string>;
        qualifiedName?: Maybe<string>;
        sourceUrl?: Maybe<string>;
        sourceRef?: Maybe<string>;
        businessAttributeDataType?: Maybe<string>;
        externalUrl?: Maybe<string>;
        createdOn?: Maybe<ResolvedAuditStamp>;
    }>;
    globalTags?: Maybe<GlobalTags>;
    glossaryTerms?: Maybe<GlossaryTerms>;
    ownership?: Maybe<Ownership>;
    domain?: Maybe<DomainAssociation>;
    applications?: Maybe<ApplicationAssociation[]>;
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
    settings?: Maybe<AssetSettings>;

    // Data job / data process instance
    lastRun?: Maybe<DataProcessInstance>;
    lastRunEvent?: Maybe<DataProcessRunEvent>;

    // Logical models
    logicalParent?: Maybe<Entity>;
    physicalChildren?: Maybe<EntityRelationshipsResult>;
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
    CHANGE_HISTORY,
}

export type EntityContextType = {
    urn: string;
    entityType: EntityType;
    dataNotCombinedWithSiblings: any;
    entityData: GenericEntityProperties | null;
    rootEntityData?: GenericEntityProperties | null;
    loading: boolean;
    baseEntity: any;
    updateEntity?: UpdateEntityType<any> | null;
    routeToTab: (params: { tabName: string; tabParams?: Record<string, any>; method?: 'push' | 'replace' }) => void;
    refetch: () => Promise<any>;
    refetchForms?: () => Promise<any>;
    lineage?: FetchedEntity | undefined;
    shouldRefetchEmbeddedListSearch?: boolean;
    setShouldRefetchEmbeddedListSearch?: React.Dispatch<React.SetStateAction<boolean>>;
    entityState?: EntityState;
    setDrawer?: React.Dispatch<React.SetStateAction<DrawerType | undefined>>;
};

export type SchemaContextType = {
    refetch?: () => Promise<any>;
};

export type EntityAndType = {
    urn: string;
    type: EntityType;
};
