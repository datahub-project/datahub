import { FetchResult, MutationFunctionOptions } from '@apollo/client';

import {
    ActionRequest,
    BrowsePathV2,
    ChartStatsSummary,
    Container,
    CustomPropertiesEntry,
    DashboardStatsSummary,
    DataJobInputOutput,
    DataPlatform,
    DataPlatformInstance,
    DatasetEditableProperties,
    DatasetEditablePropertiesUpdate,
    DatasetStatsSummary,
    Deprecation,
    DisplayProperties,
    DomainAssociation,
    EditableSchemaMetadata,
    EditableSchemaMetadataUpdate,
    Embed,
    EntityLineageResult,
    EntityPrivileges,
    EntityRelationshipsResult,
    EntityType,
    FabricType,
    FineGrainedLineage,
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
    SchemaMetadata,
    SiblingProperties,
    Status,
    SubTypes,
} from '../../../types.generated';
import { FetchedEntity } from '../../lineage/types';

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
    name: string;
    component: React.FunctionComponent<EntityTabProps>;
    icon?: React.FunctionComponent<any>;
    display?: {
        visible: (GenericEntityProperties, T) => boolean; // Whether the tab is visible on the UI. Defaults to true.
        enabled: (GenericEntityProperties, T) => boolean; // Whether the tab is enabled on the UI. Defaults to true.
    };
    properties?: any;
    id?: string;
    getDynamicName?: (GenericEntityProperties, T) => string;
};


export type EntitySidebarTab = {
    name: string;
    component: React.FunctionComponent<EntityTabProps>;
    icon: React.FunctionComponent<any>;
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
    properties?: Maybe<{
        description?: Maybe<string>;
        qualifiedName?: Maybe<string>;
        sourceUrl?: Maybe<string>;
        sourceRef?: Maybe<string>;
    }>;
    globalTags?: Maybe<GlobalTags>;
    glossaryTerms?: Maybe<GlossaryTerms>;
    ownership?: Maybe<Ownership>;
    domain?: Maybe<DomainAssociation>;
    dataProduct?: Maybe<EntityRelationshipsResult>;
    platform?: Maybe<DataPlatform>;
    dataPlatformInstance?: Maybe<DataPlatformInstance>;
    customProperties?: Maybe<CustomPropertiesEntry[]>;
    institutionalMemory?: Maybe<InstitutionalMemory>;
    schemaMetadata?: Maybe<SchemaMetadata>;
    externalUrl?: Maybe<string>;
    // to indicate something is a Stream, View instead of Dataset... etc
    entityTypeOverride?: Maybe<string>;
    /** Dataset specific- TODO, migrate these out */
    editableSchemaMetadata?: Maybe<EditableSchemaMetadata>;
    editableProperties?: Maybe<DatasetEditableProperties>;
    autoRenderAspects?: Maybe<Array<RawAspect>>;
    upstream?: Maybe<EntityLineageResult>;
    downstream?: Maybe<EntityLineageResult>;
    subTypes?: Maybe<SubTypes>;
    entityCount?: number;
    container?: Maybe<Container>;
    health?: Maybe<Array<Health>>;
    status?: Maybe<Status>;
    deprecation?: Maybe<Deprecation>;
    siblings?: Maybe<SiblingProperties>;
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
    browsePathV2?: Maybe<BrowsePathV2>;
    inputOutput?: Maybe<DataJobInputOutput>;
    tagProposals?: Maybe<ActionRequest[]>;
    termProposals?: Maybe<ActionRequest[]>;
    statsSummary?: Maybe<ChartStatsSummary | DashboardStatsSummary | DatasetStatsSummary>;
    displayProperties?: Maybe<DisplayProperties>;
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
    dataNotCombinedWithSiblings: any;
    entityData: GenericEntityProperties | null;
    loading: boolean;
    baseEntity: any;
    updateEntity?: UpdateEntityType<any> | null;
    routeToTab: (params: { tabName: string; tabParams?: Record<string, any>; method?: 'push' | 'replace' }) => void;
    refetch: () => Promise<any>;
    lineage: FetchedEntity | undefined;
    shouldRefetchEmbeddedListSearch?: boolean;
    setShouldRefetchEmbeddedListSearch?: React.Dispatch<React.SetStateAction<boolean>>;
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
