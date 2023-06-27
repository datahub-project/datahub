/* eslint-disable */
import * as Types from '../types.generated';

import {
    PlatformFieldsFragment,
    OwnershipFieldsFragment,
    GlobalTagsFieldsFragment,
    GlossaryTermsFragment,
    EntityDomainFragment,
    NonRecursiveDataFlowFieldsFragment,
    InstitutionalMemoryFieldsFragment,
    DeprecationFieldsFragment,
    EmbedFieldsFragment,
    DataPlatformInstanceFieldsFragment,
    ParentContainersFieldsFragment,
    InputFieldsFieldsFragment,
    EntityContainerFragment,
    ParentNodesFieldsFragment,
    GlossaryNodeFragment,
    NonRecursiveMlFeatureTableFragment,
    NonRecursiveMlFeatureFragment,
    NonRecursiveMlPrimaryKeyFragment,
    SchemaMetadataFieldsFragment,
    NonConflictingPlatformFieldsFragment,
} from './fragments.generated';
import { gql } from '@apollo/client';
import {
    PlatformFieldsFragmentDoc,
    OwnershipFieldsFragmentDoc,
    GlobalTagsFieldsFragmentDoc,
    GlossaryTermsFragmentDoc,
    EntityDomainFragmentDoc,
    NonRecursiveDataFlowFieldsFragmentDoc,
    InstitutionalMemoryFieldsFragmentDoc,
    DeprecationFieldsFragmentDoc,
    EmbedFieldsFragmentDoc,
    DataPlatformInstanceFieldsFragmentDoc,
    ParentContainersFieldsFragmentDoc,
    InputFieldsFieldsFragmentDoc,
    EntityContainerFragmentDoc,
    ParentNodesFieldsFragmentDoc,
    GlossaryNodeFragmentDoc,
    NonRecursiveMlFeatureTableFragmentDoc,
    NonRecursiveMlFeatureFragmentDoc,
    NonRecursiveMlPrimaryKeyFragmentDoc,
    SchemaMetadataFieldsFragmentDoc,
    NonConflictingPlatformFieldsFragmentDoc,
} from './fragments.generated';
import * as Apollo from '@apollo/client';
export type AutoCompleteFields_AccessTokenMetadata_Fragment = { __typename?: 'AccessTokenMetadata' } & Pick<
    Types.AccessTokenMetadata,
    'urn' | 'type'
>;

export type AutoCompleteFields_Assertion_Fragment = { __typename?: 'Assertion' } & Pick<
    Types.Assertion,
    'urn' | 'type'
>;

export type AutoCompleteFields_Chart_Fragment = { __typename?: 'Chart' } & Pick<
    Types.Chart,
    'chartId' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<{ __typename?: 'ChartProperties' } & Pick<Types.ChartProperties, 'name'>>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        parentContainers?: Types.Maybe<{ __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment>;
    };

export type AutoCompleteFields_Container_Fragment = { __typename?: 'Container' } & Pick<
    Types.Container,
    'urn' | 'type'
> & {
        properties?: Types.Maybe<{ __typename?: 'ContainerProperties' } & Pick<Types.ContainerProperties, 'name'>>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        parentContainers?: Types.Maybe<{ __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment>;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
    };

export type AutoCompleteFields_CorpGroup_Fragment = { __typename?: 'CorpGroup' } & Pick<
    Types.CorpGroup,
    'name' | 'urn' | 'type'
> & { info?: Types.Maybe<{ __typename?: 'CorpGroupInfo' } & Pick<Types.CorpGroupInfo, 'displayName'>> };

export type AutoCompleteFields_CorpUser_Fragment = { __typename?: 'CorpUser' } & Pick<
    Types.CorpUser,
    'username' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'CorpUserProperties' } & Pick<
                Types.CorpUserProperties,
                'displayName' | 'title' | 'firstName' | 'lastName' | 'fullName'
            >
        >;
        editableProperties?: Types.Maybe<
            { __typename?: 'CorpUserEditableProperties' } & Pick<Types.CorpUserEditableProperties, 'displayName'>
        >;
    };

export type AutoCompleteFields_Dashboard_Fragment = { __typename?: 'Dashboard' } & Pick<
    Types.Dashboard,
    'urn' | 'type'
> & {
        properties?: Types.Maybe<{ __typename?: 'DashboardProperties' } & Pick<Types.DashboardProperties, 'name'>>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        parentContainers?: Types.Maybe<{ __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment>;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
    };

export type AutoCompleteFields_DataFlow_Fragment = { __typename?: 'DataFlow' } & Pick<
    Types.DataFlow,
    'orchestrator' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<{ __typename?: 'DataFlowProperties' } & Pick<Types.DataFlowProperties, 'name'>>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
    };

export type AutoCompleteFields_DataHubPolicy_Fragment = { __typename?: 'DataHubPolicy' } & Pick<
    Types.DataHubPolicy,
    'urn' | 'type'
>;

export type AutoCompleteFields_DataHubRole_Fragment = { __typename?: 'DataHubRole' } & Pick<
    Types.DataHubRole,
    'urn' | 'type'
>;

export type AutoCompleteFields_DataHubView_Fragment = { __typename?: 'DataHubView' } & Pick<
    Types.DataHubView,
    'urn' | 'type'
>;

export type AutoCompleteFields_DataJob_Fragment = { __typename?: 'DataJob' } & Pick<
    Types.DataJob,
    'jobId' | 'urn' | 'type'
> & {
        dataFlow?: Types.Maybe<
            { __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'orchestrator'> & {
                    platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                    dataPlatformInstance?: Types.Maybe<
                        { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
                    >;
                }
        >;
        properties?: Types.Maybe<{ __typename?: 'DataJobProperties' } & Pick<Types.DataJobProperties, 'name'>>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
    };

export type AutoCompleteFields_DataPlatform_Fragment = { __typename?: 'DataPlatform' } & Pick<
    Types.DataPlatform,
    'urn' | 'type'
> &
    NonConflictingPlatformFieldsFragment;

export type AutoCompleteFields_DataPlatformInstance_Fragment = { __typename?: 'DataPlatformInstance' } & Pick<
    Types.DataPlatformInstance,
    'urn' | 'type'
>;

export type AutoCompleteFields_DataProcessInstance_Fragment = { __typename?: 'DataProcessInstance' } & Pick<
    Types.DataProcessInstance,
    'urn' | 'type'
>;

export type AutoCompleteFields_Dataset_Fragment = { __typename?: 'Dataset' } & Pick<
    Types.Dataset,
    'name' | 'urn' | 'type'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        properties?: Types.Maybe<
            { __typename?: 'DatasetProperties' } & Pick<Types.DatasetProperties, 'name' | 'qualifiedName'>
        >;
        parentContainers?: Types.Maybe<{ __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment>;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
    } & DatasetStatsFieldsFragment;

export type AutoCompleteFields_Domain_Fragment = { __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'> & {
        properties?: Types.Maybe<{ __typename?: 'DomainProperties' } & Pick<Types.DomainProperties, 'name'>>;
    };

export type AutoCompleteFields_GlossaryNode_Fragment = { __typename?: 'GlossaryNode' } & Pick<
    Types.GlossaryNode,
    'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'GlossaryNodeProperties' } & Pick<Types.GlossaryNodeProperties, 'name'>
        >;
    };

export type AutoCompleteFields_GlossaryTerm_Fragment = { __typename?: 'GlossaryTerm' } & Pick<
    Types.GlossaryTerm,
    'name' | 'hierarchicalName' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'GlossaryTermProperties' } & Pick<Types.GlossaryTermProperties, 'name'>
        >;
    };

export type AutoCompleteFields_MlFeature_Fragment = { __typename?: 'MLFeature' } & Pick<
    Types.MlFeature,
    'name' | 'urn' | 'type'
> & {
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
    };

export type AutoCompleteFields_MlFeatureTable_Fragment = { __typename?: 'MLFeatureTable' } & Pick<
    Types.MlFeatureTable,
    'name' | 'urn' | 'type'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
    };

export type AutoCompleteFields_MlModel_Fragment = { __typename?: 'MLModel' } & Pick<
    Types.MlModel,
    'name' | 'urn' | 'type'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
    };

export type AutoCompleteFields_MlModelGroup_Fragment = { __typename?: 'MLModelGroup' } & Pick<
    Types.MlModelGroup,
    'name' | 'urn' | 'type'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
    };

export type AutoCompleteFields_MlPrimaryKey_Fragment = { __typename?: 'MLPrimaryKey' } & Pick<
    Types.MlPrimaryKey,
    'name' | 'urn' | 'type'
> & {
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
    };

export type AutoCompleteFields_Notebook_Fragment = { __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>;

export type AutoCompleteFields_Post_Fragment = { __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>;

export type AutoCompleteFields_QueryEntity_Fragment = { __typename?: 'QueryEntity' } & Pick<
    Types.QueryEntity,
    'urn' | 'type'
>;

export type AutoCompleteFields_SchemaFieldEntity_Fragment = { __typename?: 'SchemaFieldEntity' } & Pick<
    Types.SchemaFieldEntity,
    'urn' | 'type'
>;

export type AutoCompleteFields_Tag_Fragment = { __typename?: 'Tag' } & Pick<Types.Tag, 'name' | 'urn' | 'type'> & {
        properties?: Types.Maybe<{ __typename?: 'TagProperties' } & Pick<Types.TagProperties, 'name' | 'colorHex'>>;
    };

export type AutoCompleteFields_Test_Fragment = { __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>;

export type AutoCompleteFields_VersionedDataset_Fragment = { __typename?: 'VersionedDataset' } & Pick<
    Types.VersionedDataset,
    'urn' | 'type'
>;

export type AutoCompleteFieldsFragment =
    | AutoCompleteFields_AccessTokenMetadata_Fragment
    | AutoCompleteFields_Assertion_Fragment
    | AutoCompleteFields_Chart_Fragment
    | AutoCompleteFields_Container_Fragment
    | AutoCompleteFields_CorpGroup_Fragment
    | AutoCompleteFields_CorpUser_Fragment
    | AutoCompleteFields_Dashboard_Fragment
    | AutoCompleteFields_DataFlow_Fragment
    | AutoCompleteFields_DataHubPolicy_Fragment
    | AutoCompleteFields_DataHubRole_Fragment
    | AutoCompleteFields_DataHubView_Fragment
    | AutoCompleteFields_DataJob_Fragment
    | AutoCompleteFields_DataPlatform_Fragment
    | AutoCompleteFields_DataPlatformInstance_Fragment
    | AutoCompleteFields_DataProcessInstance_Fragment
    | AutoCompleteFields_Dataset_Fragment
    | AutoCompleteFields_Domain_Fragment
    | AutoCompleteFields_GlossaryNode_Fragment
    | AutoCompleteFields_GlossaryTerm_Fragment
    | AutoCompleteFields_MlFeature_Fragment
    | AutoCompleteFields_MlFeatureTable_Fragment
    | AutoCompleteFields_MlModel_Fragment
    | AutoCompleteFields_MlModelGroup_Fragment
    | AutoCompleteFields_MlPrimaryKey_Fragment
    | AutoCompleteFields_Notebook_Fragment
    | AutoCompleteFields_Post_Fragment
    | AutoCompleteFields_QueryEntity_Fragment
    | AutoCompleteFields_SchemaFieldEntity_Fragment
    | AutoCompleteFields_Tag_Fragment
    | AutoCompleteFields_Test_Fragment
    | AutoCompleteFields_VersionedDataset_Fragment;

export type GetAutoCompleteResultsQueryVariables = Types.Exact<{
    input: Types.AutoCompleteInput;
}>;

export type GetAutoCompleteResultsQuery = { __typename?: 'Query' } & {
    autoComplete?: Types.Maybe<
        { __typename?: 'AutoCompleteResults' } & Pick<Types.AutoCompleteResults, 'query' | 'suggestions'> & {
                entities: Array<
                    | ({ __typename?: 'AccessTokenMetadata' } & AutoCompleteFields_AccessTokenMetadata_Fragment)
                    | ({ __typename?: 'Assertion' } & AutoCompleteFields_Assertion_Fragment)
                    | ({ __typename?: 'Chart' } & AutoCompleteFields_Chart_Fragment)
                    | ({ __typename?: 'Container' } & AutoCompleteFields_Container_Fragment)
                    | ({ __typename?: 'CorpGroup' } & AutoCompleteFields_CorpGroup_Fragment)
                    | ({ __typename?: 'CorpUser' } & AutoCompleteFields_CorpUser_Fragment)
                    | ({ __typename?: 'Dashboard' } & AutoCompleteFields_Dashboard_Fragment)
                    | ({ __typename?: 'DataFlow' } & AutoCompleteFields_DataFlow_Fragment)
                    | ({ __typename?: 'DataHubPolicy' } & AutoCompleteFields_DataHubPolicy_Fragment)
                    | ({ __typename?: 'DataHubRole' } & AutoCompleteFields_DataHubRole_Fragment)
                    | ({ __typename?: 'DataHubView' } & AutoCompleteFields_DataHubView_Fragment)
                    | ({ __typename?: 'DataJob' } & AutoCompleteFields_DataJob_Fragment)
                    | ({ __typename?: 'DataPlatform' } & AutoCompleteFields_DataPlatform_Fragment)
                    | ({ __typename?: 'DataPlatformInstance' } & AutoCompleteFields_DataPlatformInstance_Fragment)
                    | ({ __typename?: 'DataProcessInstance' } & AutoCompleteFields_DataProcessInstance_Fragment)
                    | ({ __typename?: 'Dataset' } & AutoCompleteFields_Dataset_Fragment)
                    | ({ __typename?: 'Domain' } & AutoCompleteFields_Domain_Fragment)
                    | ({ __typename?: 'GlossaryNode' } & AutoCompleteFields_GlossaryNode_Fragment)
                    | ({ __typename?: 'GlossaryTerm' } & AutoCompleteFields_GlossaryTerm_Fragment)
                    | ({ __typename?: 'MLFeature' } & AutoCompleteFields_MlFeature_Fragment)
                    | ({ __typename?: 'MLFeatureTable' } & AutoCompleteFields_MlFeatureTable_Fragment)
                    | ({ __typename?: 'MLModel' } & AutoCompleteFields_MlModel_Fragment)
                    | ({ __typename?: 'MLModelGroup' } & AutoCompleteFields_MlModelGroup_Fragment)
                    | ({ __typename?: 'MLPrimaryKey' } & AutoCompleteFields_MlPrimaryKey_Fragment)
                    | ({ __typename?: 'Notebook' } & AutoCompleteFields_Notebook_Fragment)
                    | ({ __typename?: 'Post' } & AutoCompleteFields_Post_Fragment)
                    | ({ __typename?: 'QueryEntity' } & AutoCompleteFields_QueryEntity_Fragment)
                    | ({ __typename?: 'SchemaFieldEntity' } & AutoCompleteFields_SchemaFieldEntity_Fragment)
                    | ({ __typename?: 'Tag' } & AutoCompleteFields_Tag_Fragment)
                    | ({ __typename?: 'Test' } & AutoCompleteFields_Test_Fragment)
                    | ({ __typename?: 'VersionedDataset' } & AutoCompleteFields_VersionedDataset_Fragment)
                >;
            }
    >;
};

export type GetAutoCompleteMultipleResultsQueryVariables = Types.Exact<{
    input: Types.AutoCompleteMultipleInput;
}>;

export type GetAutoCompleteMultipleResultsQuery = { __typename?: 'Query' } & {
    autoCompleteForMultiple?: Types.Maybe<
        { __typename?: 'AutoCompleteMultipleResults' } & Pick<Types.AutoCompleteMultipleResults, 'query'> & {
                suggestions: Array<
                    { __typename?: 'AutoCompleteResultForEntity' } & Pick<
                        Types.AutoCompleteResultForEntity,
                        'type' | 'suggestions'
                    > & {
                            entities: Array<
                                | ({
                                      __typename?: 'AccessTokenMetadata';
                                  } & AutoCompleteFields_AccessTokenMetadata_Fragment)
                                | ({ __typename?: 'Assertion' } & AutoCompleteFields_Assertion_Fragment)
                                | ({ __typename?: 'Chart' } & AutoCompleteFields_Chart_Fragment)
                                | ({ __typename?: 'Container' } & AutoCompleteFields_Container_Fragment)
                                | ({ __typename?: 'CorpGroup' } & AutoCompleteFields_CorpGroup_Fragment)
                                | ({ __typename?: 'CorpUser' } & AutoCompleteFields_CorpUser_Fragment)
                                | ({ __typename?: 'Dashboard' } & AutoCompleteFields_Dashboard_Fragment)
                                | ({ __typename?: 'DataFlow' } & AutoCompleteFields_DataFlow_Fragment)
                                | ({ __typename?: 'DataHubPolicy' } & AutoCompleteFields_DataHubPolicy_Fragment)
                                | ({ __typename?: 'DataHubRole' } & AutoCompleteFields_DataHubRole_Fragment)
                                | ({ __typename?: 'DataHubView' } & AutoCompleteFields_DataHubView_Fragment)
                                | ({ __typename?: 'DataJob' } & AutoCompleteFields_DataJob_Fragment)
                                | ({ __typename?: 'DataPlatform' } & AutoCompleteFields_DataPlatform_Fragment)
                                | ({
                                      __typename?: 'DataPlatformInstance';
                                  } & AutoCompleteFields_DataPlatformInstance_Fragment)
                                | ({
                                      __typename?: 'DataProcessInstance';
                                  } & AutoCompleteFields_DataProcessInstance_Fragment)
                                | ({ __typename?: 'Dataset' } & AutoCompleteFields_Dataset_Fragment)
                                | ({ __typename?: 'Domain' } & AutoCompleteFields_Domain_Fragment)
                                | ({ __typename?: 'GlossaryNode' } & AutoCompleteFields_GlossaryNode_Fragment)
                                | ({ __typename?: 'GlossaryTerm' } & AutoCompleteFields_GlossaryTerm_Fragment)
                                | ({ __typename?: 'MLFeature' } & AutoCompleteFields_MlFeature_Fragment)
                                | ({ __typename?: 'MLFeatureTable' } & AutoCompleteFields_MlFeatureTable_Fragment)
                                | ({ __typename?: 'MLModel' } & AutoCompleteFields_MlModel_Fragment)
                                | ({ __typename?: 'MLModelGroup' } & AutoCompleteFields_MlModelGroup_Fragment)
                                | ({ __typename?: 'MLPrimaryKey' } & AutoCompleteFields_MlPrimaryKey_Fragment)
                                | ({ __typename?: 'Notebook' } & AutoCompleteFields_Notebook_Fragment)
                                | ({ __typename?: 'Post' } & AutoCompleteFields_Post_Fragment)
                                | ({ __typename?: 'QueryEntity' } & AutoCompleteFields_QueryEntity_Fragment)
                                | ({ __typename?: 'SchemaFieldEntity' } & AutoCompleteFields_SchemaFieldEntity_Fragment)
                                | ({ __typename?: 'Tag' } & AutoCompleteFields_Tag_Fragment)
                                | ({ __typename?: 'Test' } & AutoCompleteFields_Test_Fragment)
                                | ({ __typename?: 'VersionedDataset' } & AutoCompleteFields_VersionedDataset_Fragment)
                            >;
                        }
                >;
            }
    >;
};

export type DatasetStatsFieldsFragment = { __typename?: 'Dataset' } & {
    lastProfile?: Types.Maybe<
        Array<
            { __typename?: 'DatasetProfile' } & Pick<
                Types.DatasetProfile,
                'rowCount' | 'columnCount' | 'sizeInBytes' | 'timestampMillis'
            >
        >
    >;
    lastOperation?: Types.Maybe<
        Array<{ __typename?: 'Operation' } & Pick<Types.Operation, 'lastUpdatedTimestamp' | 'timestampMillis'>>
    >;
    statsSummary?: Types.Maybe<
        { __typename?: 'DatasetStatsSummary' } & Pick<
            Types.DatasetStatsSummary,
            'queryCountLast30Days' | 'uniqueUserCountLast30Days'
        > & {
                topUsersLast30Days?: Types.Maybe<
                    Array<
                        { __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type' | 'username'> & {
                                properties?: Types.Maybe<
                                    { __typename?: 'CorpUserProperties' } & Pick<
                                        Types.CorpUserProperties,
                                        'displayName' | 'firstName' | 'lastName' | 'fullName'
                                    >
                                >;
                                editableProperties?: Types.Maybe<
                                    { __typename?: 'CorpUserEditableProperties' } & Pick<
                                        Types.CorpUserEditableProperties,
                                        'displayName' | 'pictureLink'
                                    >
                                >;
                            }
                    >
                >;
            }
    >;
};

export type SearchResultFields_AccessTokenMetadata_Fragment = { __typename?: 'AccessTokenMetadata' } & Pick<
    Types.AccessTokenMetadata,
    'urn' | 'type'
>;

export type SearchResultFields_Assertion_Fragment = { __typename?: 'Assertion' } & Pick<
    Types.Assertion,
    'urn' | 'type'
>;

export type SearchResultFields_Chart_Fragment = { __typename?: 'Chart' } & Pick<
    Types.Chart,
    'chartId' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'ChartProperties' } & Pick<
                Types.ChartProperties,
                'name' | 'description' | 'externalUrl' | 'type' | 'access'
            > & {
                    lastModified: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'>;
                    created: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'>;
                }
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'ChartEditableProperties' } & Pick<Types.ChartEditableProperties, 'description'>
        >;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        parentContainers?: Types.Maybe<{ __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment>;
        statsSummary?: Types.Maybe<
            { __typename?: 'ChartStatsSummary' } & Pick<
                Types.ChartStatsSummary,
                'viewCount' | 'uniqueUserCountLast30Days'
            > & {
                    topUsersLast30Days?: Types.Maybe<
                        Array<
                            { __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type' | 'username'> & {
                                    properties?: Types.Maybe<
                                        { __typename?: 'CorpUserProperties' } & Pick<
                                            Types.CorpUserProperties,
                                            'displayName' | 'firstName' | 'lastName' | 'fullName'
                                        >
                                    >;
                                    editableProperties?: Types.Maybe<
                                        { __typename?: 'CorpUserEditableProperties' } & Pick<
                                            Types.CorpUserEditableProperties,
                                            'displayName' | 'pictureLink'
                                        >
                                    >;
                                }
                        >
                    >;
                }
        >;
    };

export type SearchResultFields_Container_Fragment = { __typename?: 'Container' } & Pick<
    Types.Container,
    'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'ContainerProperties' } & Pick<
                Types.ContainerProperties,
                'name' | 'description' | 'externalUrl'
            >
        >;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        editableProperties?: Types.Maybe<
            { __typename?: 'ContainerEditableProperties' } & Pick<Types.ContainerEditableProperties, 'description'>
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        tags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
        entities?: Types.Maybe<{ __typename?: 'SearchResults' } & Pick<Types.SearchResults, 'total'>>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        parentContainers?: Types.Maybe<{ __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment>;
    };

export type SearchResultFields_CorpGroup_Fragment = { __typename?: 'CorpGroup' } & Pick<
    Types.CorpGroup,
    'name' | 'urn' | 'type'
> & {
        info?: Types.Maybe<{ __typename?: 'CorpGroupInfo' } & Pick<Types.CorpGroupInfo, 'displayName' | 'description'>>;
        memberCount?: Types.Maybe<
            { __typename?: 'EntityRelationshipsResult' } & Pick<Types.EntityRelationshipsResult, 'total'>
        >;
    };

export type SearchResultFields_CorpUser_Fragment = { __typename?: 'CorpUser' } & Pick<
    Types.CorpUser,
    'username' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'CorpUserProperties' } & Pick<
                Types.CorpUserProperties,
                'active' | 'displayName' | 'title' | 'firstName' | 'lastName' | 'fullName' | 'email'
            >
        >;
        info?: Types.Maybe<
            { __typename?: 'CorpUserInfo' } & Pick<
                Types.CorpUserInfo,
                'active' | 'displayName' | 'title' | 'firstName' | 'lastName' | 'fullName' | 'email'
            >
        >;
        editableProperties?: Types.Maybe<
            { __typename?: 'CorpUserEditableProperties' } & Pick<
                Types.CorpUserEditableProperties,
                'displayName' | 'title' | 'pictureLink'
            >
        >;
    };

export type SearchResultFields_Dashboard_Fragment = { __typename?: 'Dashboard' } & Pick<
    Types.Dashboard,
    'dashboardId' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'DashboardProperties' } & Pick<
                Types.DashboardProperties,
                'name' | 'description' | 'externalUrl' | 'access'
            > & { lastModified: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'> }
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'DashboardEditableProperties' } & Pick<Types.DashboardEditableProperties, 'description'>
        >;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        parentContainers?: Types.Maybe<{ __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment>;
        statsSummary?: Types.Maybe<
            { __typename?: 'DashboardStatsSummary' } & Pick<
                Types.DashboardStatsSummary,
                'viewCount' | 'uniqueUserCountLast30Days'
            > & {
                    topUsersLast30Days?: Types.Maybe<
                        Array<
                            { __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type' | 'username'> & {
                                    properties?: Types.Maybe<
                                        { __typename?: 'CorpUserProperties' } & Pick<
                                            Types.CorpUserProperties,
                                            'displayName' | 'firstName' | 'lastName' | 'fullName'
                                        >
                                    >;
                                    editableProperties?: Types.Maybe<
                                        { __typename?: 'CorpUserEditableProperties' } & Pick<
                                            Types.CorpUserEditableProperties,
                                            'displayName' | 'pictureLink'
                                        >
                                    >;
                                }
                        >
                    >;
                }
        >;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
    };

export type SearchResultFields_DataFlow_Fragment = { __typename?: 'DataFlow' } & Pick<
    Types.DataFlow,
    'flowId' | 'cluster' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'DataFlowProperties' } & Pick<
                Types.DataFlowProperties,
                'name' | 'description' | 'project' | 'externalUrl'
            >
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'DataFlowEditableProperties' } & Pick<Types.DataFlowEditableProperties, 'description'>
        >;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        childJobs?: Types.Maybe<
            { __typename?: 'EntityRelationshipsResult' } & Pick<Types.EntityRelationshipsResult, 'total'>
        >;
    };

export type SearchResultFields_DataHubPolicy_Fragment = { __typename?: 'DataHubPolicy' } & Pick<
    Types.DataHubPolicy,
    'urn' | 'type'
>;

export type SearchResultFields_DataHubRole_Fragment = { __typename?: 'DataHubRole' } & Pick<
    Types.DataHubRole,
    'urn' | 'type'
>;

export type SearchResultFields_DataHubView_Fragment = { __typename?: 'DataHubView' } & Pick<
    Types.DataHubView,
    'urn' | 'type'
>;

export type SearchResultFields_DataJob_Fragment = { __typename?: 'DataJob' } & Pick<
    Types.DataJob,
    'jobId' | 'urn' | 'type'
> & {
        dataFlow?: Types.Maybe<{ __typename?: 'DataFlow' } & NonRecursiveDataFlowFieldsFragment>;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        properties?: Types.Maybe<
            { __typename?: 'DataJobProperties' } & Pick<Types.DataJobProperties, 'name' | 'description' | 'externalUrl'>
        >;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'DataJobEditableProperties' } & Pick<Types.DataJobEditableProperties, 'description'>
        >;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        lastRun?: Types.Maybe<
            { __typename?: 'DataProcessInstanceResult' } & Pick<
                Types.DataProcessInstanceResult,
                'count' | 'start' | 'total'
            > & {
                    runs?: Types.Maybe<
                        Array<
                            Types.Maybe<
                                { __typename?: 'DataProcessInstance' } & Pick<
                                    Types.DataProcessInstance,
                                    'urn' | 'type'
                                > & {
                                        created?: Types.Maybe<
                                            { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time' | 'actor'>
                                        >;
                                    }
                            >
                        >
                    >;
                }
        >;
    };

export type SearchResultFields_DataPlatform_Fragment = { __typename?: 'DataPlatform' } & Pick<
    Types.DataPlatform,
    'urn' | 'type'
> &
    NonConflictingPlatformFieldsFragment;

export type SearchResultFields_DataPlatformInstance_Fragment = { __typename?: 'DataPlatformInstance' } & Pick<
    Types.DataPlatformInstance,
    'urn' | 'type'
>;

export type SearchResultFields_DataProcessInstance_Fragment = { __typename?: 'DataProcessInstance' } & Pick<
    Types.DataProcessInstance,
    'urn' | 'type'
>;

export type SearchResultFields_Dataset_Fragment = { __typename?: 'Dataset' } & Pick<
    Types.Dataset,
    'name' | 'origin' | 'uri' | 'platformNativeType' | 'urn' | 'type'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        editableProperties?: Types.Maybe<
            { __typename?: 'DatasetEditableProperties' } & Pick<Types.DatasetEditableProperties, 'description'>
        >;
        properties?: Types.Maybe<
            { __typename?: 'DatasetProperties' } & Pick<
                Types.DatasetProperties,
                'name' | 'description' | 'qualifiedName' | 'externalUrl'
            > & {
                    customProperties?: Types.Maybe<
                        Array<
                            { __typename?: 'CustomPropertiesEntry' } & Pick<
                                Types.CustomPropertiesEntry,
                                'key' | 'value'
                            >
                        >
                    >;
                }
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        parentContainers?: Types.Maybe<{ __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        siblings?: Types.Maybe<
            { __typename?: 'SiblingProperties' } & Pick<Types.SiblingProperties, 'isPrimary'> & {
                    siblings?: Types.Maybe<
                        Array<
                            Types.Maybe<
                                | ({ __typename?: 'AccessTokenMetadata' } & Pick<
                                      Types.AccessTokenMetadata,
                                      'urn' | 'type'
                                  >)
                                | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'>)
                                | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'>)
                                | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
                                | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
                                | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'>)
                                | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'>)
                                | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'>)
                                | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
                                | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
                                | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
                                | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'>)
                                | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
                                | ({ __typename?: 'DataPlatformInstance' } & Pick<
                                      Types.DataPlatformInstance,
                                      'urn' | 'type'
                                  >)
                                | ({ __typename?: 'DataProcessInstance' } & Pick<
                                      Types.DataProcessInstance,
                                      'urn' | 'type'
                                  >)
                                | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'name' | 'urn' | 'type'> & {
                                          platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                                          properties?: Types.Maybe<
                                              { __typename?: 'DatasetProperties' } & Pick<
                                                  Types.DatasetProperties,
                                                  'name' | 'description' | 'qualifiedName' | 'externalUrl'
                                              >
                                          >;
                                      })
                                | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'>)
                                | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'>)
                                | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'>)
                                | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'>)
                                | ({ __typename?: 'MLFeatureTable' } & Pick<Types.MlFeatureTable, 'urn' | 'type'>)
                                | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'>)
                                | ({ __typename?: 'MLModelGroup' } & Pick<Types.MlModelGroup, 'urn' | 'type'>)
                                | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'>)
                                | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>)
                                | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>)
                                | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'>)
                                | ({ __typename?: 'SchemaFieldEntity' } & Pick<Types.SchemaFieldEntity, 'urn' | 'type'>)
                                | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type'>)
                                | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>)
                                | ({ __typename?: 'VersionedDataset' } & Pick<Types.VersionedDataset, 'urn' | 'type'>)
                            >
                        >
                    >;
                }
        >;
    } & DatasetStatsFieldsFragment;

export type SearchResultFields_Domain_Fragment = { __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'> & {
        properties?: Types.Maybe<
            { __typename?: 'DomainProperties' } & Pick<Types.DomainProperties, 'name' | 'description'>
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
    };

export type SearchResultFields_GlossaryNode_Fragment = { __typename?: 'GlossaryNode' } & Pick<
    Types.GlossaryNode,
    'urn' | 'type'
> & {
        parentNodes?: Types.Maybe<{ __typename?: 'ParentNodesResult' } & ParentNodesFieldsFragment>;
    } & GlossaryNodeFragment;

export type SearchResultFields_GlossaryTerm_Fragment = { __typename?: 'GlossaryTerm' } & Pick<
    Types.GlossaryTerm,
    'name' | 'hierarchicalName' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'GlossaryTermProperties' } & Pick<
                Types.GlossaryTermProperties,
                'name' | 'description' | 'termSource' | 'sourceRef' | 'sourceUrl' | 'rawSchema'
            > & {
                    customProperties?: Types.Maybe<
                        Array<
                            { __typename?: 'CustomPropertiesEntry' } & Pick<
                                Types.CustomPropertiesEntry,
                                'key' | 'value'
                            >
                        >
                    >;
                }
        >;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        parentNodes?: Types.Maybe<{ __typename?: 'ParentNodesResult' } & ParentNodesFieldsFragment>;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
    };

export type SearchResultFields_MlFeature_Fragment = { __typename?: 'MLFeature' } & Pick<
    Types.MlFeature,
    'urn' | 'type'
> &
    NonRecursiveMlFeatureFragment;

export type SearchResultFields_MlFeatureTable_Fragment = { __typename?: 'MLFeatureTable' } & Pick<
    Types.MlFeatureTable,
    'name' | 'description' | 'urn' | 'type'
> & {
        featureTableProperties?: Types.Maybe<
            { __typename?: 'MLFeatureTableProperties' } & Pick<Types.MlFeatureTableProperties, 'description'> & {
                    mlFeatures?: Types.Maybe<
                        Array<Types.Maybe<{ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn'>>>
                    >;
                    mlPrimaryKeys?: Types.Maybe<
                        Array<Types.Maybe<{ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn'>>>
                    >;
                }
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
    };

export type SearchResultFields_MlModel_Fragment = { __typename?: 'MLModel' } & Pick<
    Types.MlModel,
    'name' | 'description' | 'origin' | 'urn' | 'type'
> & {
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
    };

export type SearchResultFields_MlModelGroup_Fragment = { __typename?: 'MLModelGroup' } & Pick<
    Types.MlModelGroup,
    'name' | 'origin' | 'description' | 'urn' | 'type'
> & {
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
    };

export type SearchResultFields_MlPrimaryKey_Fragment = { __typename?: 'MLPrimaryKey' } & Pick<
    Types.MlPrimaryKey,
    'urn' | 'type'
> &
    NonRecursiveMlPrimaryKeyFragment;

export type SearchResultFields_Notebook_Fragment = { __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>;

export type SearchResultFields_Post_Fragment = { __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>;

export type SearchResultFields_QueryEntity_Fragment = { __typename?: 'QueryEntity' } & Pick<
    Types.QueryEntity,
    'urn' | 'type'
>;

export type SearchResultFields_SchemaFieldEntity_Fragment = { __typename?: 'SchemaFieldEntity' } & Pick<
    Types.SchemaFieldEntity,
    'urn' | 'type'
>;

export type SearchResultFields_Tag_Fragment = { __typename?: 'Tag' } & Pick<
    Types.Tag,
    'name' | 'description' | 'urn' | 'type'
> & { properties?: Types.Maybe<{ __typename?: 'TagProperties' } & Pick<Types.TagProperties, 'name' | 'colorHex'>> };

export type SearchResultFields_Test_Fragment = { __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>;

export type SearchResultFields_VersionedDataset_Fragment = { __typename?: 'VersionedDataset' } & Pick<
    Types.VersionedDataset,
    'urn' | 'type'
>;

export type SearchResultFieldsFragment =
    | SearchResultFields_AccessTokenMetadata_Fragment
    | SearchResultFields_Assertion_Fragment
    | SearchResultFields_Chart_Fragment
    | SearchResultFields_Container_Fragment
    | SearchResultFields_CorpGroup_Fragment
    | SearchResultFields_CorpUser_Fragment
    | SearchResultFields_Dashboard_Fragment
    | SearchResultFields_DataFlow_Fragment
    | SearchResultFields_DataHubPolicy_Fragment
    | SearchResultFields_DataHubRole_Fragment
    | SearchResultFields_DataHubView_Fragment
    | SearchResultFields_DataJob_Fragment
    | SearchResultFields_DataPlatform_Fragment
    | SearchResultFields_DataPlatformInstance_Fragment
    | SearchResultFields_DataProcessInstance_Fragment
    | SearchResultFields_Dataset_Fragment
    | SearchResultFields_Domain_Fragment
    | SearchResultFields_GlossaryNode_Fragment
    | SearchResultFields_GlossaryTerm_Fragment
    | SearchResultFields_MlFeature_Fragment
    | SearchResultFields_MlFeatureTable_Fragment
    | SearchResultFields_MlModel_Fragment
    | SearchResultFields_MlModelGroup_Fragment
    | SearchResultFields_MlPrimaryKey_Fragment
    | SearchResultFields_Notebook_Fragment
    | SearchResultFields_Post_Fragment
    | SearchResultFields_QueryEntity_Fragment
    | SearchResultFields_SchemaFieldEntity_Fragment
    | SearchResultFields_Tag_Fragment
    | SearchResultFields_Test_Fragment
    | SearchResultFields_VersionedDataset_Fragment;

export type FacetFieldsFragment = { __typename?: 'FacetMetadata' } & Pick<
    Types.FacetMetadata,
    'field' | 'displayName'
> & {
        aggregations: Array<
            { __typename?: 'AggregationMetadata' } & Pick<Types.AggregationMetadata, 'value' | 'count'> & {
                    entity?: Types.Maybe<
                        | ({ __typename?: 'AccessTokenMetadata' } & Pick<Types.AccessTokenMetadata, 'urn' | 'type'>)
                        | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'>)
                        | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'>)
                        | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'> & {
                                  platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                                  properties?: Types.Maybe<
                                      { __typename?: 'ContainerProperties' } & Pick<Types.ContainerProperties, 'name'>
                                  >;
                              })
                        | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'name' | 'urn' | 'type'> & {
                                  properties?: Types.Maybe<
                                      { __typename?: 'CorpGroupProperties' } & Pick<
                                          Types.CorpGroupProperties,
                                          'displayName'
                                      >
                                  >;
                              })
                        | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'username' | 'urn' | 'type'> & {
                                  properties?: Types.Maybe<
                                      { __typename?: 'CorpUserProperties' } & Pick<
                                          Types.CorpUserProperties,
                                          'displayName' | 'fullName'
                                      >
                                  >;
                                  editableProperties?: Types.Maybe<
                                      { __typename?: 'CorpUserEditableProperties' } & Pick<
                                          Types.CorpUserEditableProperties,
                                          'displayName' | 'pictureLink'
                                      >
                                  >;
                              })
                        | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'>)
                        | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'>)
                        | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
                        | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
                        | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
                        | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'>)
                        | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'> &
                              PlatformFieldsFragment)
                        | ({ __typename?: 'DataPlatformInstance' } & Pick<Types.DataPlatformInstance, 'urn' | 'type'> &
                              DataPlatformInstanceFieldsFragment)
                        | ({ __typename?: 'DataProcessInstance' } & Pick<Types.DataProcessInstance, 'urn' | 'type'>)
                        | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'>)
                        | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'> & {
                                  properties?: Types.Maybe<
                                      { __typename?: 'DomainProperties' } & Pick<Types.DomainProperties, 'name'>
                                  >;
                              })
                        | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'>)
                        | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'name' | 'urn' | 'type'> & {
                                  properties?: Types.Maybe<
                                      { __typename?: 'GlossaryTermProperties' } & Pick<
                                          Types.GlossaryTermProperties,
                                          'name'
                                      >
                                  >;
                              })
                        | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'>)
                        | ({ __typename?: 'MLFeatureTable' } & Pick<Types.MlFeatureTable, 'urn' | 'type'>)
                        | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'>)
                        | ({ __typename?: 'MLModelGroup' } & Pick<Types.MlModelGroup, 'urn' | 'type'>)
                        | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'>)
                        | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>)
                        | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>)
                        | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'>)
                        | ({ __typename?: 'SchemaFieldEntity' } & Pick<Types.SchemaFieldEntity, 'urn' | 'type'>)
                        | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'name' | 'urn' | 'type'> & {
                                  properties?: Types.Maybe<
                                      { __typename?: 'TagProperties' } & Pick<Types.TagProperties, 'name' | 'colorHex'>
                                  >;
                              })
                        | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>)
                        | ({ __typename?: 'VersionedDataset' } & Pick<Types.VersionedDataset, 'urn' | 'type'>)
                    >;
                }
        >;
    };

export type SearchResultsFragment = { __typename?: 'SearchResults' } & Pick<
    Types.SearchResults,
    'start' | 'count' | 'total'
> & {
        searchResults: Array<
            { __typename?: 'SearchResult' } & {
                entity:
                    | ({ __typename?: 'AccessTokenMetadata' } & SearchResultFields_AccessTokenMetadata_Fragment)
                    | ({ __typename?: 'Assertion' } & SearchResultFields_Assertion_Fragment)
                    | ({ __typename?: 'Chart' } & SearchResultFields_Chart_Fragment)
                    | ({ __typename?: 'Container' } & SearchResultFields_Container_Fragment)
                    | ({ __typename?: 'CorpGroup' } & SearchResultFields_CorpGroup_Fragment)
                    | ({ __typename?: 'CorpUser' } & SearchResultFields_CorpUser_Fragment)
                    | ({ __typename?: 'Dashboard' } & SearchResultFields_Dashboard_Fragment)
                    | ({ __typename?: 'DataFlow' } & SearchResultFields_DataFlow_Fragment)
                    | ({ __typename?: 'DataHubPolicy' } & SearchResultFields_DataHubPolicy_Fragment)
                    | ({ __typename?: 'DataHubRole' } & SearchResultFields_DataHubRole_Fragment)
                    | ({ __typename?: 'DataHubView' } & SearchResultFields_DataHubView_Fragment)
                    | ({ __typename?: 'DataJob' } & SearchResultFields_DataJob_Fragment)
                    | ({ __typename?: 'DataPlatform' } & SearchResultFields_DataPlatform_Fragment)
                    | ({ __typename?: 'DataPlatformInstance' } & SearchResultFields_DataPlatformInstance_Fragment)
                    | ({ __typename?: 'DataProcessInstance' } & SearchResultFields_DataProcessInstance_Fragment)
                    | ({ __typename?: 'Dataset' } & SearchResultFields_Dataset_Fragment)
                    | ({ __typename?: 'Domain' } & SearchResultFields_Domain_Fragment)
                    | ({ __typename?: 'GlossaryNode' } & SearchResultFields_GlossaryNode_Fragment)
                    | ({ __typename?: 'GlossaryTerm' } & SearchResultFields_GlossaryTerm_Fragment)
                    | ({ __typename?: 'MLFeature' } & SearchResultFields_MlFeature_Fragment)
                    | ({ __typename?: 'MLFeatureTable' } & SearchResultFields_MlFeatureTable_Fragment)
                    | ({ __typename?: 'MLModel' } & SearchResultFields_MlModel_Fragment)
                    | ({ __typename?: 'MLModelGroup' } & SearchResultFields_MlModelGroup_Fragment)
                    | ({ __typename?: 'MLPrimaryKey' } & SearchResultFields_MlPrimaryKey_Fragment)
                    | ({ __typename?: 'Notebook' } & SearchResultFields_Notebook_Fragment)
                    | ({ __typename?: 'Post' } & SearchResultFields_Post_Fragment)
                    | ({ __typename?: 'QueryEntity' } & SearchResultFields_QueryEntity_Fragment)
                    | ({ __typename?: 'SchemaFieldEntity' } & SearchResultFields_SchemaFieldEntity_Fragment)
                    | ({ __typename?: 'Tag' } & SearchResultFields_Tag_Fragment)
                    | ({ __typename?: 'Test' } & SearchResultFields_Test_Fragment)
                    | ({ __typename?: 'VersionedDataset' } & SearchResultFields_VersionedDataset_Fragment);
                matchedFields: Array<{ __typename?: 'MatchedField' } & Pick<Types.MatchedField, 'name' | 'value'>>;
                insights?: Types.Maybe<
                    Array<{ __typename?: 'SearchInsight' } & Pick<Types.SearchInsight, 'text' | 'icon'>>
                >;
            }
        >;
        facets?: Types.Maybe<Array<{ __typename?: 'FacetMetadata' } & FacetFieldsFragment>>;
    };

export type SchemaFieldEntityFieldsFragment = { __typename?: 'SchemaFieldEntity' } & Pick<
    Types.SchemaFieldEntity,
    'urn' | 'type' | 'fieldPath'
> & {
        parent:
            | ({ __typename?: 'AccessTokenMetadata' } & SearchResultFields_AccessTokenMetadata_Fragment)
            | ({ __typename?: 'Assertion' } & SearchResultFields_Assertion_Fragment)
            | ({ __typename?: 'Chart' } & SearchResultFields_Chart_Fragment)
            | ({ __typename?: 'Container' } & SearchResultFields_Container_Fragment)
            | ({ __typename?: 'CorpGroup' } & SearchResultFields_CorpGroup_Fragment)
            | ({ __typename?: 'CorpUser' } & SearchResultFields_CorpUser_Fragment)
            | ({ __typename?: 'Dashboard' } & SearchResultFields_Dashboard_Fragment)
            | ({ __typename?: 'DataFlow' } & SearchResultFields_DataFlow_Fragment)
            | ({ __typename?: 'DataHubPolicy' } & SearchResultFields_DataHubPolicy_Fragment)
            | ({ __typename?: 'DataHubRole' } & SearchResultFields_DataHubRole_Fragment)
            | ({ __typename?: 'DataHubView' } & SearchResultFields_DataHubView_Fragment)
            | ({ __typename?: 'DataJob' } & SearchResultFields_DataJob_Fragment)
            | ({ __typename?: 'DataPlatform' } & SearchResultFields_DataPlatform_Fragment)
            | ({ __typename?: 'DataPlatformInstance' } & SearchResultFields_DataPlatformInstance_Fragment)
            | ({ __typename?: 'DataProcessInstance' } & SearchResultFields_DataProcessInstance_Fragment)
            | ({ __typename?: 'Dataset' } & SearchResultFields_Dataset_Fragment)
            | ({ __typename?: 'Domain' } & SearchResultFields_Domain_Fragment)
            | ({ __typename?: 'GlossaryNode' } & SearchResultFields_GlossaryNode_Fragment)
            | ({ __typename?: 'GlossaryTerm' } & SearchResultFields_GlossaryTerm_Fragment)
            | ({ __typename?: 'MLFeature' } & SearchResultFields_MlFeature_Fragment)
            | ({ __typename?: 'MLFeatureTable' } & SearchResultFields_MlFeatureTable_Fragment)
            | ({ __typename?: 'MLModel' } & SearchResultFields_MlModel_Fragment)
            | ({ __typename?: 'MLModelGroup' } & SearchResultFields_MlModelGroup_Fragment)
            | ({ __typename?: 'MLPrimaryKey' } & SearchResultFields_MlPrimaryKey_Fragment)
            | ({ __typename?: 'Notebook' } & SearchResultFields_Notebook_Fragment)
            | ({ __typename?: 'Post' } & SearchResultFields_Post_Fragment)
            | ({ __typename?: 'QueryEntity' } & SearchResultFields_QueryEntity_Fragment)
            | ({ __typename?: 'SchemaFieldEntity' } & SearchResultFields_SchemaFieldEntity_Fragment)
            | ({ __typename?: 'Tag' } & SearchResultFields_Tag_Fragment)
            | ({ __typename?: 'Test' } & SearchResultFields_Test_Fragment)
            | ({ __typename?: 'VersionedDataset' } & SearchResultFields_VersionedDataset_Fragment);
    };

export type SearchAcrossRelationshipResultsFragment = { __typename?: 'SearchAcrossLineageResults' } & Pick<
    Types.SearchAcrossLineageResults,
    'start' | 'count' | 'total'
> & {
        searchResults: Array<
            { __typename?: 'SearchAcrossLineageResult' } & Pick<Types.SearchAcrossLineageResult, 'degree'> & {
                    entity:
                        | ({ __typename?: 'AccessTokenMetadata' } & SearchResultFields_AccessTokenMetadata_Fragment)
                        | ({ __typename?: 'Assertion' } & SearchResultFields_Assertion_Fragment)
                        | ({ __typename?: 'Chart' } & SearchResultFields_Chart_Fragment)
                        | ({ __typename?: 'Container' } & SearchResultFields_Container_Fragment)
                        | ({ __typename?: 'CorpGroup' } & SearchResultFields_CorpGroup_Fragment)
                        | ({ __typename?: 'CorpUser' } & SearchResultFields_CorpUser_Fragment)
                        | ({ __typename?: 'Dashboard' } & SearchResultFields_Dashboard_Fragment)
                        | ({ __typename?: 'DataFlow' } & SearchResultFields_DataFlow_Fragment)
                        | ({ __typename?: 'DataHubPolicy' } & SearchResultFields_DataHubPolicy_Fragment)
                        | ({ __typename?: 'DataHubRole' } & SearchResultFields_DataHubRole_Fragment)
                        | ({ __typename?: 'DataHubView' } & SearchResultFields_DataHubView_Fragment)
                        | ({ __typename?: 'DataJob' } & SearchResultFields_DataJob_Fragment)
                        | ({ __typename?: 'DataPlatform' } & SearchResultFields_DataPlatform_Fragment)
                        | ({ __typename?: 'DataPlatformInstance' } & SearchResultFields_DataPlatformInstance_Fragment)
                        | ({ __typename?: 'DataProcessInstance' } & SearchResultFields_DataProcessInstance_Fragment)
                        | ({ __typename?: 'Dataset' } & {
                              assertions?: Types.Maybe<
                                  { __typename?: 'EntityAssertionsResult' } & {
                                      assertions: Array<
                                          { __typename?: 'Assertion' } & {
                                              runEvents?: Types.Maybe<
                                                  { __typename?: 'AssertionRunEventsResult' } & Pick<
                                                      Types.AssertionRunEventsResult,
                                                      'total' | 'failed' | 'succeeded'
                                                  >
                                              >;
                                          }
                                      >;
                                  }
                              >;
                          } & SearchResultFields_Dataset_Fragment)
                        | ({ __typename?: 'Domain' } & SearchResultFields_Domain_Fragment)
                        | ({ __typename?: 'GlossaryNode' } & SearchResultFields_GlossaryNode_Fragment)
                        | ({ __typename?: 'GlossaryTerm' } & SearchResultFields_GlossaryTerm_Fragment)
                        | ({ __typename?: 'MLFeature' } & SearchResultFields_MlFeature_Fragment)
                        | ({ __typename?: 'MLFeatureTable' } & SearchResultFields_MlFeatureTable_Fragment)
                        | ({ __typename?: 'MLModel' } & SearchResultFields_MlModel_Fragment)
                        | ({ __typename?: 'MLModelGroup' } & SearchResultFields_MlModelGroup_Fragment)
                        | ({ __typename?: 'MLPrimaryKey' } & SearchResultFields_MlPrimaryKey_Fragment)
                        | ({ __typename?: 'Notebook' } & SearchResultFields_Notebook_Fragment)
                        | ({ __typename?: 'Post' } & SearchResultFields_Post_Fragment)
                        | ({ __typename?: 'QueryEntity' } & SearchResultFields_QueryEntity_Fragment)
                        | ({ __typename?: 'SchemaFieldEntity' } & SearchResultFields_SchemaFieldEntity_Fragment)
                        | ({ __typename?: 'Tag' } & SearchResultFields_Tag_Fragment)
                        | ({ __typename?: 'Test' } & SearchResultFields_Test_Fragment)
                        | ({ __typename?: 'VersionedDataset' } & SearchResultFields_VersionedDataset_Fragment);
                    matchedFields: Array<{ __typename?: 'MatchedField' } & Pick<Types.MatchedField, 'name' | 'value'>>;
                    insights?: Types.Maybe<
                        Array<{ __typename?: 'SearchInsight' } & Pick<Types.SearchInsight, 'text' | 'icon'>>
                    >;
                    paths?: Types.Maybe<
                        Array<
                            Types.Maybe<
                                { __typename?: 'EntityPath' } & {
                                    path?: Types.Maybe<
                                        Array<
                                            Types.Maybe<
                                                | ({
                                                      __typename?: 'AccessTokenMetadata';
                                                  } & SearchResultFields_AccessTokenMetadata_Fragment)
                                                | ({ __typename?: 'Assertion' } & SearchResultFields_Assertion_Fragment)
                                                | ({ __typename?: 'Chart' } & SearchResultFields_Chart_Fragment)
                                                | ({ __typename?: 'Container' } & SearchResultFields_Container_Fragment)
                                                | ({ __typename?: 'CorpGroup' } & SearchResultFields_CorpGroup_Fragment)
                                                | ({ __typename?: 'CorpUser' } & SearchResultFields_CorpUser_Fragment)
                                                | ({ __typename?: 'Dashboard' } & SearchResultFields_Dashboard_Fragment)
                                                | ({ __typename?: 'DataFlow' } & SearchResultFields_DataFlow_Fragment)
                                                | ({
                                                      __typename?: 'DataHubPolicy';
                                                  } & SearchResultFields_DataHubPolicy_Fragment)
                                                | ({
                                                      __typename?: 'DataHubRole';
                                                  } & SearchResultFields_DataHubRole_Fragment)
                                                | ({
                                                      __typename?: 'DataHubView';
                                                  } & SearchResultFields_DataHubView_Fragment)
                                                | ({ __typename?: 'DataJob' } & SearchResultFields_DataJob_Fragment)
                                                | ({
                                                      __typename?: 'DataPlatform';
                                                  } & SearchResultFields_DataPlatform_Fragment)
                                                | ({
                                                      __typename?: 'DataPlatformInstance';
                                                  } & SearchResultFields_DataPlatformInstance_Fragment)
                                                | ({
                                                      __typename?: 'DataProcessInstance';
                                                  } & SearchResultFields_DataProcessInstance_Fragment)
                                                | ({ __typename?: 'Dataset' } & SearchResultFields_Dataset_Fragment)
                                                | ({ __typename?: 'Domain' } & SearchResultFields_Domain_Fragment)
                                                | ({
                                                      __typename?: 'GlossaryNode';
                                                  } & SearchResultFields_GlossaryNode_Fragment)
                                                | ({
                                                      __typename?: 'GlossaryTerm';
                                                  } & SearchResultFields_GlossaryTerm_Fragment)
                                                | ({ __typename?: 'MLFeature' } & SearchResultFields_MlFeature_Fragment)
                                                | ({
                                                      __typename?: 'MLFeatureTable';
                                                  } & SearchResultFields_MlFeatureTable_Fragment)
                                                | ({ __typename?: 'MLModel' } & SearchResultFields_MlModel_Fragment)
                                                | ({
                                                      __typename?: 'MLModelGroup';
                                                  } & SearchResultFields_MlModelGroup_Fragment)
                                                | ({
                                                      __typename?: 'MLPrimaryKey';
                                                  } & SearchResultFields_MlPrimaryKey_Fragment)
                                                | ({ __typename?: 'Notebook' } & SearchResultFields_Notebook_Fragment)
                                                | ({ __typename?: 'Post' } & SearchResultFields_Post_Fragment)
                                                | ({
                                                      __typename?: 'QueryEntity';
                                                  } & SearchResultFields_QueryEntity_Fragment)
                                                | ({
                                                      __typename?: 'SchemaFieldEntity';
                                                  } & SchemaFieldEntityFieldsFragment &
                                                      SearchResultFields_SchemaFieldEntity_Fragment)
                                                | ({ __typename?: 'Tag' } & SearchResultFields_Tag_Fragment)
                                                | ({ __typename?: 'Test' } & SearchResultFields_Test_Fragment)
                                                | ({
                                                      __typename?: 'VersionedDataset';
                                                  } & SearchResultFields_VersionedDataset_Fragment)
                                            >
                                        >
                                    >;
                                }
                            >
                        >
                    >;
                }
        >;
        facets?: Types.Maybe<Array<{ __typename?: 'FacetMetadata' } & FacetFieldsFragment>>;
    };

export type GetSearchResultsQueryVariables = Types.Exact<{
    input: Types.SearchInput;
}>;

export type GetSearchResultsQuery = { __typename?: 'Query' } & {
    search?: Types.Maybe<{ __typename?: 'SearchResults' } & SearchResultsFragment>;
};

export type GetSearchResultsForMultipleQueryVariables = Types.Exact<{
    input: Types.SearchAcrossEntitiesInput;
}>;

export type GetSearchResultsForMultipleQuery = { __typename?: 'Query' } & {
    searchAcrossEntities?: Types.Maybe<{ __typename?: 'SearchResults' } & SearchResultsFragment>;
};

export type SearchAcrossLineageQueryVariables = Types.Exact<{
    input: Types.SearchAcrossLineageInput;
    includeAssertions?: Types.Maybe<Types.Scalars['Boolean']>;
}>;

export type SearchAcrossLineageQuery = { __typename?: 'Query' } & {
    searchAcrossLineage?: Types.Maybe<
        { __typename?: 'SearchAcrossLineageResults' } & SearchAcrossRelationshipResultsFragment
    >;
};

export type GetEntityMentionNodeQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetEntityMentionNodeQuery = { __typename?: 'Query' } & {
    entity?: Types.Maybe<
        | ({ __typename?: 'AccessTokenMetadata' } & Pick<Types.AccessTokenMetadata, 'urn' | 'type'> &
              SearchResultFields_AccessTokenMetadata_Fragment)
        | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'> & SearchResultFields_Assertion_Fragment)
        | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'> & SearchResultFields_Chart_Fragment)
        | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'> & SearchResultFields_Container_Fragment)
        | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'> & SearchResultFields_CorpGroup_Fragment)
        | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'> & SearchResultFields_CorpUser_Fragment)
        | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'> & SearchResultFields_Dashboard_Fragment)
        | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'> & SearchResultFields_DataFlow_Fragment)
        | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'> &
              SearchResultFields_DataHubPolicy_Fragment)
        | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'> &
              SearchResultFields_DataHubRole_Fragment)
        | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'> &
              SearchResultFields_DataHubView_Fragment)
        | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'> & SearchResultFields_DataJob_Fragment)
        | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'> &
              SearchResultFields_DataPlatform_Fragment)
        | ({ __typename?: 'DataPlatformInstance' } & Pick<Types.DataPlatformInstance, 'urn' | 'type'> &
              SearchResultFields_DataPlatformInstance_Fragment)
        | ({ __typename?: 'DataProcessInstance' } & Pick<Types.DataProcessInstance, 'urn' | 'type'> &
              SearchResultFields_DataProcessInstance_Fragment)
        | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'> & SearchResultFields_Dataset_Fragment)
        | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'> & SearchResultFields_Domain_Fragment)
        | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'> &
              SearchResultFields_GlossaryNode_Fragment)
        | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'> &
              SearchResultFields_GlossaryTerm_Fragment)
        | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'> & SearchResultFields_MlFeature_Fragment)
        | ({ __typename?: 'MLFeatureTable' } & Pick<Types.MlFeatureTable, 'urn' | 'type'> &
              SearchResultFields_MlFeatureTable_Fragment)
        | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'> & SearchResultFields_MlModel_Fragment)
        | ({ __typename?: 'MLModelGroup' } & Pick<Types.MlModelGroup, 'urn' | 'type'> &
              SearchResultFields_MlModelGroup_Fragment)
        | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'> &
              SearchResultFields_MlPrimaryKey_Fragment)
        | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'> & SearchResultFields_Notebook_Fragment)
        | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'> & SearchResultFields_Post_Fragment)
        | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'> &
              SearchResultFields_QueryEntity_Fragment)
        | ({ __typename?: 'SchemaFieldEntity' } & Pick<Types.SchemaFieldEntity, 'urn' | 'type'> &
              SearchResultFields_SchemaFieldEntity_Fragment)
        | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type'> & SearchResultFields_Tag_Fragment)
        | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'> & SearchResultFields_Test_Fragment)
        | ({ __typename?: 'VersionedDataset' } & Pick<Types.VersionedDataset, 'urn' | 'type'> &
              SearchResultFields_VersionedDataset_Fragment)
    >;
};

export const DatasetStatsFieldsFragmentDoc = gql`
    fragment datasetStatsFields on Dataset {
        lastProfile: datasetProfiles(limit: 1) {
            rowCount
            columnCount
            sizeInBytes
            timestampMillis
        }
        lastOperation: operations(limit: 1) {
            lastUpdatedTimestamp
            timestampMillis
        }
        statsSummary {
            queryCountLast30Days
            uniqueUserCountLast30Days
            topUsersLast30Days {
                urn
                type
                username
                properties {
                    displayName
                    firstName
                    lastName
                    fullName
                }
                editableProperties {
                    displayName
                    pictureLink
                }
            }
        }
    }
`;
export const AutoCompleteFieldsFragmentDoc = gql`
    fragment autoCompleteFields on Entity {
        urn
        type
        ... on Dataset {
            name
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            properties {
                name
                qualifiedName
            }
            parentContainers {
                ...parentContainersFields
            }
            subTypes {
                typeNames
            }
            ...datasetStatsFields
        }
        ... on CorpUser {
            username
            properties {
                displayName
                title
                firstName
                lastName
                fullName
            }
            editableProperties {
                displayName
            }
        }
        ... on CorpGroup {
            name
            info {
                displayName
            }
        }
        ... on Dashboard {
            properties {
                name
            }
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            parentContainers {
                ...parentContainersFields
            }
            subTypes {
                typeNames
            }
        }
        ... on Chart {
            chartId
            properties {
                name
            }
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            parentContainers {
                ...parentContainersFields
            }
        }
        ... on DataFlow {
            orchestrator
            properties {
                name
            }
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
        }
        ... on DataJob {
            dataFlow {
                orchestrator
                platform {
                    ...platformFields
                }
                dataPlatformInstance {
                    ...dataPlatformInstanceFields
                }
            }
            jobId
            properties {
                name
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
        }
        ... on GlossaryTerm {
            name
            hierarchicalName
            properties {
                name
            }
        }
        ... on GlossaryNode {
            properties {
                name
            }
        }
        ... on Domain {
            properties {
                name
            }
        }
        ... on Container {
            properties {
                name
            }
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            parentContainers {
                ...parentContainersFields
            }
            subTypes {
                typeNames
            }
        }
        ... on Tag {
            name
            properties {
                name
                colorHex
            }
        }
        ... on MLFeatureTable {
            name
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
        }
        ... on MLFeature {
            name
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
        }
        ... on MLPrimaryKey {
            name
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
        }
        ... on MLModel {
            name
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
        }
        ... on MLModelGroup {
            name
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
        }
        ... on DataPlatform {
            ...nonConflictingPlatformFields
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
    ${ParentContainersFieldsFragmentDoc}
    ${DatasetStatsFieldsFragmentDoc}
    ${NonConflictingPlatformFieldsFragmentDoc}
`;
export const SearchResultFieldsFragmentDoc = gql`
    fragment searchResultFields on Entity {
        urn
        type
        ... on Dataset {
            name
            origin
            uri
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            editableProperties {
                description
            }
            platformNativeType
            properties {
                name
                description
                qualifiedName
                customProperties {
                    key
                    value
                }
                externalUrl
            }
            ownership {
                ...ownershipFields
            }
            globalTags {
                ...globalTagsFields
            }
            glossaryTerms {
                ...glossaryTerms
            }
            subTypes {
                typeNames
            }
            domain {
                ...entityDomain
            }
            parentContainers {
                ...parentContainersFields
            }
            deprecation {
                ...deprecationFields
            }
            siblings {
                isPrimary
                siblings {
                    urn
                    type
                    ... on Dataset {
                        platform {
                            ...platformFields
                        }
                        name
                        properties {
                            name
                            description
                            qualifiedName
                            externalUrl
                        }
                    }
                }
            }
            ...datasetStatsFields
        }
        ... on CorpUser {
            username
            properties {
                active
                displayName
                title
                firstName
                lastName
                fullName
                email
            }
            info {
                active
                displayName
                title
                firstName
                lastName
                fullName
                email
            }
            editableProperties {
                displayName
                title
                pictureLink
            }
        }
        ... on CorpGroup {
            name
            info {
                displayName
                description
            }
            memberCount: relationships(
                input: { types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"], direction: INCOMING, start: 0, count: 1 }
            ) {
                total
            }
        }
        ... on Dashboard {
            dashboardId
            properties {
                name
                description
                externalUrl
                access
                lastModified {
                    time
                }
            }
            ownership {
                ...ownershipFields
            }
            globalTags {
                ...globalTagsFields
            }
            glossaryTerms {
                ...glossaryTerms
            }
            editableProperties {
                description
            }
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            domain {
                ...entityDomain
            }
            deprecation {
                ...deprecationFields
            }
            parentContainers {
                ...parentContainersFields
            }
            statsSummary {
                viewCount
                uniqueUserCountLast30Days
                topUsersLast30Days {
                    urn
                    type
                    username
                    properties {
                        displayName
                        firstName
                        lastName
                        fullName
                    }
                    editableProperties {
                        displayName
                        pictureLink
                    }
                }
            }
            subTypes {
                typeNames
            }
        }
        ... on Chart {
            chartId
            properties {
                name
                description
                externalUrl
                type
                access
                lastModified {
                    time
                }
                created {
                    time
                }
            }
            ownership {
                ...ownershipFields
            }
            globalTags {
                ...globalTagsFields
            }
            glossaryTerms {
                ...glossaryTerms
            }
            editableProperties {
                description
            }
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            domain {
                ...entityDomain
            }
            deprecation {
                ...deprecationFields
            }
            parentContainers {
                ...parentContainersFields
            }
            statsSummary {
                viewCount
                uniqueUserCountLast30Days
                topUsersLast30Days {
                    urn
                    type
                    username
                    properties {
                        displayName
                        firstName
                        lastName
                        fullName
                    }
                    editableProperties {
                        displayName
                        pictureLink
                    }
                }
            }
        }
        ... on DataFlow {
            flowId
            cluster
            properties {
                name
                description
                project
                externalUrl
            }
            ownership {
                ...ownershipFields
            }
            globalTags {
                ...globalTagsFields
            }
            glossaryTerms {
                ...glossaryTerms
            }
            editableProperties {
                description
            }
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            domain {
                ...entityDomain
            }
            deprecation {
                ...deprecationFields
            }
            childJobs: relationships(input: { types: ["IsPartOf"], direction: INCOMING, start: 0, count: 100 }) {
                total
            }
        }
        ... on DataJob {
            dataFlow {
                ...nonRecursiveDataFlowFields
            }
            jobId
            ownership {
                ...ownershipFields
            }
            properties {
                name
                description
                externalUrl
            }
            globalTags {
                ...globalTagsFields
            }
            glossaryTerms {
                ...glossaryTerms
            }
            editableProperties {
                description
            }
            domain {
                ...entityDomain
            }
            deprecation {
                ...deprecationFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            lastRun: runs(start: 0, count: 1) {
                count
                start
                total
                runs {
                    urn
                    type
                    created {
                        time
                        actor
                    }
                }
            }
        }
        ... on GlossaryTerm {
            name
            hierarchicalName
            properties {
                name
                description
                termSource
                sourceRef
                sourceUrl
                rawSchema
                customProperties {
                    key
                    value
                }
            }
            deprecation {
                ...deprecationFields
            }
            parentNodes {
                ...parentNodesFields
            }
            domain {
                ...entityDomain
            }
        }
        ... on GlossaryNode {
            ...glossaryNode
            parentNodes {
                ...parentNodesFields
            }
        }
        ... on Domain {
            properties {
                name
                description
            }
            ownership {
                ...ownershipFields
            }
        }
        ... on Container {
            properties {
                name
                description
                externalUrl
            }
            platform {
                ...platformFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            editableProperties {
                description
            }
            ownership {
                ...ownershipFields
            }
            tags {
                ...globalTagsFields
            }
            glossaryTerms {
                ...glossaryTerms
            }
            subTypes {
                typeNames
            }
            entities(input: {}) {
                total
            }
            deprecation {
                ...deprecationFields
            }
            parentContainers {
                ...parentContainersFields
            }
        }
        ... on MLFeatureTable {
            name
            description
            featureTableProperties {
                description
                mlFeatures {
                    urn
                }
                mlPrimaryKeys {
                    urn
                }
            }
            ownership {
                ...ownershipFields
            }
            platform {
                ...platformFields
            }
            deprecation {
                ...deprecationFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
        }
        ... on MLFeature {
            ...nonRecursiveMLFeature
        }
        ... on MLPrimaryKey {
            ...nonRecursiveMLPrimaryKey
        }
        ... on MLModel {
            name
            description
            origin
            ownership {
                ...ownershipFields
            }
            platform {
                ...platformFields
            }
            deprecation {
                ...deprecationFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
        }
        ... on MLModelGroup {
            name
            origin
            description
            ownership {
                ...ownershipFields
            }
            platform {
                ...platformFields
            }
            deprecation {
                ...deprecationFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
        }
        ... on Tag {
            name
            properties {
                name
                colorHex
            }
            description
        }
        ... on DataPlatform {
            ...nonConflictingPlatformFields
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${ParentContainersFieldsFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${DatasetStatsFieldsFragmentDoc}
    ${NonRecursiveDataFlowFieldsFragmentDoc}
    ${ParentNodesFieldsFragmentDoc}
    ${GlossaryNodeFragmentDoc}
    ${NonRecursiveMlFeatureFragmentDoc}
    ${NonRecursiveMlPrimaryKeyFragmentDoc}
    ${NonConflictingPlatformFieldsFragmentDoc}
`;
export const FacetFieldsFragmentDoc = gql`
    fragment facetFields on FacetMetadata {
        field
        displayName
        aggregations {
            value
            count
            entity {
                urn
                type
                ... on Tag {
                    name
                    properties {
                        name
                        colorHex
                    }
                }
                ... on GlossaryTerm {
                    name
                    properties {
                        name
                    }
                }
                ... on DataPlatform {
                    ...platformFields
                }
                ... on DataPlatformInstance {
                    ...dataPlatformInstanceFields
                }
                ... on Domain {
                    properties {
                        name
                    }
                }
                ... on Container {
                    platform {
                        ...platformFields
                    }
                    properties {
                        name
                    }
                }
                ... on CorpUser {
                    username
                    properties {
                        displayName
                        fullName
                    }
                    editableProperties {
                        displayName
                        pictureLink
                    }
                }
                ... on CorpGroup {
                    name
                    properties {
                        displayName
                    }
                }
            }
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
`;
export const SearchResultsFragmentDoc = gql`
    fragment searchResults on SearchResults {
        start
        count
        total
        searchResults {
            entity {
                ...searchResultFields
            }
            matchedFields {
                name
                value
            }
            insights {
                text
                icon
            }
        }
        facets {
            ...facetFields
        }
    }
    ${SearchResultFieldsFragmentDoc}
    ${FacetFieldsFragmentDoc}
`;
export const SchemaFieldEntityFieldsFragmentDoc = gql`
    fragment schemaFieldEntityFields on SchemaFieldEntity {
        urn
        type
        fieldPath
        parent {
            ...searchResultFields
        }
    }
    ${SearchResultFieldsFragmentDoc}
`;
export const SearchAcrossRelationshipResultsFragmentDoc = gql`
    fragment searchAcrossRelationshipResults on SearchAcrossLineageResults {
        start
        count
        total
        searchResults {
            entity {
                ...searchResultFields
                ... on Dataset {
                    assertions @include(if: $includeAssertions) {
                        assertions {
                            runEvents(limit: 1) {
                                total
                                failed
                                succeeded
                            }
                        }
                    }
                }
            }
            matchedFields {
                name
                value
            }
            insights {
                text
                icon
            }
            paths {
                path {
                    ...searchResultFields
                    ... on SchemaFieldEntity {
                        ...schemaFieldEntityFields
                    }
                }
            }
            degree
        }
        facets {
            ...facetFields
        }
    }
    ${SearchResultFieldsFragmentDoc}
    ${SchemaFieldEntityFieldsFragmentDoc}
    ${FacetFieldsFragmentDoc}
`;
export const GetAutoCompleteResultsDocument = gql`
    query getAutoCompleteResults($input: AutoCompleteInput!) {
        autoComplete(input: $input) {
            query
            suggestions
            entities {
                ...autoCompleteFields
            }
        }
    }
    ${AutoCompleteFieldsFragmentDoc}
`;

/**
 * __useGetAutoCompleteResultsQuery__
 *
 * To run a query within a React component, call `useGetAutoCompleteResultsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetAutoCompleteResultsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetAutoCompleteResultsQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetAutoCompleteResultsQuery(
    baseOptions: Apollo.QueryHookOptions<GetAutoCompleteResultsQuery, GetAutoCompleteResultsQueryVariables>,
) {
    return Apollo.useQuery<GetAutoCompleteResultsQuery, GetAutoCompleteResultsQueryVariables>(
        GetAutoCompleteResultsDocument,
        baseOptions,
    );
}
export function useGetAutoCompleteResultsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetAutoCompleteResultsQuery, GetAutoCompleteResultsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetAutoCompleteResultsQuery, GetAutoCompleteResultsQueryVariables>(
        GetAutoCompleteResultsDocument,
        baseOptions,
    );
}
export type GetAutoCompleteResultsQueryHookResult = ReturnType<typeof useGetAutoCompleteResultsQuery>;
export type GetAutoCompleteResultsLazyQueryHookResult = ReturnType<typeof useGetAutoCompleteResultsLazyQuery>;
export type GetAutoCompleteResultsQueryResult = Apollo.QueryResult<
    GetAutoCompleteResultsQuery,
    GetAutoCompleteResultsQueryVariables
>;
export const GetAutoCompleteMultipleResultsDocument = gql`
    query getAutoCompleteMultipleResults($input: AutoCompleteMultipleInput!) {
        autoCompleteForMultiple(input: $input) {
            query
            suggestions {
                type
                suggestions
                entities {
                    ...autoCompleteFields
                }
            }
        }
    }
    ${AutoCompleteFieldsFragmentDoc}
`;

/**
 * __useGetAutoCompleteMultipleResultsQuery__
 *
 * To run a query within a React component, call `useGetAutoCompleteMultipleResultsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetAutoCompleteMultipleResultsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetAutoCompleteMultipleResultsQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetAutoCompleteMultipleResultsQuery(
    baseOptions: Apollo.QueryHookOptions<
        GetAutoCompleteMultipleResultsQuery,
        GetAutoCompleteMultipleResultsQueryVariables
    >,
) {
    return Apollo.useQuery<GetAutoCompleteMultipleResultsQuery, GetAutoCompleteMultipleResultsQueryVariables>(
        GetAutoCompleteMultipleResultsDocument,
        baseOptions,
    );
}
export function useGetAutoCompleteMultipleResultsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<
        GetAutoCompleteMultipleResultsQuery,
        GetAutoCompleteMultipleResultsQueryVariables
    >,
) {
    return Apollo.useLazyQuery<GetAutoCompleteMultipleResultsQuery, GetAutoCompleteMultipleResultsQueryVariables>(
        GetAutoCompleteMultipleResultsDocument,
        baseOptions,
    );
}
export type GetAutoCompleteMultipleResultsQueryHookResult = ReturnType<typeof useGetAutoCompleteMultipleResultsQuery>;
export type GetAutoCompleteMultipleResultsLazyQueryHookResult = ReturnType<
    typeof useGetAutoCompleteMultipleResultsLazyQuery
>;
export type GetAutoCompleteMultipleResultsQueryResult = Apollo.QueryResult<
    GetAutoCompleteMultipleResultsQuery,
    GetAutoCompleteMultipleResultsQueryVariables
>;
export const GetSearchResultsDocument = gql`
    query getSearchResults($input: SearchInput!) {
        search(input: $input) {
            ...searchResults
        }
    }
    ${SearchResultsFragmentDoc}
`;

/**
 * __useGetSearchResultsQuery__
 *
 * To run a query within a React component, call `useGetSearchResultsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetSearchResultsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetSearchResultsQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetSearchResultsQuery(
    baseOptions: Apollo.QueryHookOptions<GetSearchResultsQuery, GetSearchResultsQueryVariables>,
) {
    return Apollo.useQuery<GetSearchResultsQuery, GetSearchResultsQueryVariables>(
        GetSearchResultsDocument,
        baseOptions,
    );
}
export function useGetSearchResultsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetSearchResultsQuery, GetSearchResultsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetSearchResultsQuery, GetSearchResultsQueryVariables>(
        GetSearchResultsDocument,
        baseOptions,
    );
}
export type GetSearchResultsQueryHookResult = ReturnType<typeof useGetSearchResultsQuery>;
export type GetSearchResultsLazyQueryHookResult = ReturnType<typeof useGetSearchResultsLazyQuery>;
export type GetSearchResultsQueryResult = Apollo.QueryResult<GetSearchResultsQuery, GetSearchResultsQueryVariables>;
export const GetSearchResultsForMultipleDocument = gql`
    query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
        searchAcrossEntities(input: $input) {
            ...searchResults
        }
    }
    ${SearchResultsFragmentDoc}
`;

/**
 * __useGetSearchResultsForMultipleQuery__
 *
 * To run a query within a React component, call `useGetSearchResultsForMultipleQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetSearchResultsForMultipleQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetSearchResultsForMultipleQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetSearchResultsForMultipleQuery(
    baseOptions: Apollo.QueryHookOptions<GetSearchResultsForMultipleQuery, GetSearchResultsForMultipleQueryVariables>,
) {
    return Apollo.useQuery<GetSearchResultsForMultipleQuery, GetSearchResultsForMultipleQueryVariables>(
        GetSearchResultsForMultipleDocument,
        baseOptions,
    );
}
export function useGetSearchResultsForMultipleLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<
        GetSearchResultsForMultipleQuery,
        GetSearchResultsForMultipleQueryVariables
    >,
) {
    return Apollo.useLazyQuery<GetSearchResultsForMultipleQuery, GetSearchResultsForMultipleQueryVariables>(
        GetSearchResultsForMultipleDocument,
        baseOptions,
    );
}
export type GetSearchResultsForMultipleQueryHookResult = ReturnType<typeof useGetSearchResultsForMultipleQuery>;
export type GetSearchResultsForMultipleLazyQueryHookResult = ReturnType<typeof useGetSearchResultsForMultipleLazyQuery>;
export type GetSearchResultsForMultipleQueryResult = Apollo.QueryResult<
    GetSearchResultsForMultipleQuery,
    GetSearchResultsForMultipleQueryVariables
>;
export const SearchAcrossLineageDocument = gql`
    query searchAcrossLineage($input: SearchAcrossLineageInput!, $includeAssertions: Boolean = false) {
        searchAcrossLineage(input: $input) {
            ...searchAcrossRelationshipResults
        }
    }
    ${SearchAcrossRelationshipResultsFragmentDoc}
`;

/**
 * __useSearchAcrossLineageQuery__
 *
 * To run a query within a React component, call `useSearchAcrossLineageQuery` and pass it any options that fit your needs.
 * When your component renders, `useSearchAcrossLineageQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useSearchAcrossLineageQuery({
 *   variables: {
 *      input: // value for 'input'
 *      includeAssertions: // value for 'includeAssertions'
 *   },
 * });
 */
export function useSearchAcrossLineageQuery(
    baseOptions: Apollo.QueryHookOptions<SearchAcrossLineageQuery, SearchAcrossLineageQueryVariables>,
) {
    return Apollo.useQuery<SearchAcrossLineageQuery, SearchAcrossLineageQueryVariables>(
        SearchAcrossLineageDocument,
        baseOptions,
    );
}
export function useSearchAcrossLineageLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<SearchAcrossLineageQuery, SearchAcrossLineageQueryVariables>,
) {
    return Apollo.useLazyQuery<SearchAcrossLineageQuery, SearchAcrossLineageQueryVariables>(
        SearchAcrossLineageDocument,
        baseOptions,
    );
}
export type SearchAcrossLineageQueryHookResult = ReturnType<typeof useSearchAcrossLineageQuery>;
export type SearchAcrossLineageLazyQueryHookResult = ReturnType<typeof useSearchAcrossLineageLazyQuery>;
export type SearchAcrossLineageQueryResult = Apollo.QueryResult<
    SearchAcrossLineageQuery,
    SearchAcrossLineageQueryVariables
>;
export const GetEntityMentionNodeDocument = gql`
    query getEntityMentionNode($urn: String!) {
        entity(urn: $urn) {
            urn
            type
            ...searchResultFields
        }
    }
    ${SearchResultFieldsFragmentDoc}
`;

/**
 * __useGetEntityMentionNodeQuery__
 *
 * To run a query within a React component, call `useGetEntityMentionNodeQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetEntityMentionNodeQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetEntityMentionNodeQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetEntityMentionNodeQuery(
    baseOptions: Apollo.QueryHookOptions<GetEntityMentionNodeQuery, GetEntityMentionNodeQueryVariables>,
) {
    return Apollo.useQuery<GetEntityMentionNodeQuery, GetEntityMentionNodeQueryVariables>(
        GetEntityMentionNodeDocument,
        baseOptions,
    );
}
export function useGetEntityMentionNodeLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetEntityMentionNodeQuery, GetEntityMentionNodeQueryVariables>,
) {
    return Apollo.useLazyQuery<GetEntityMentionNodeQuery, GetEntityMentionNodeQueryVariables>(
        GetEntityMentionNodeDocument,
        baseOptions,
    );
}
export type GetEntityMentionNodeQueryHookResult = ReturnType<typeof useGetEntityMentionNodeQuery>;
export type GetEntityMentionNodeLazyQueryHookResult = ReturnType<typeof useGetEntityMentionNodeLazyQuery>;
export type GetEntityMentionNodeQueryResult = Apollo.QueryResult<
    GetEntityMentionNodeQuery,
    GetEntityMentionNodeQueryVariables
>;
