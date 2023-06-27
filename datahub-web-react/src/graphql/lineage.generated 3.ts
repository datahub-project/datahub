/* eslint-disable */
import * as Types from '../types.generated';

import {
    NonRecursiveDataFlowFieldsFragment,
    OwnershipFieldsFragment,
    ParentNodesFieldsFragment,
    GlossaryNodeFragment,
    PlatformFieldsFragment,
    GlobalTagsFieldsFragment,
    GlossaryTermsFragment,
    EntityDomainFragment,
    InstitutionalMemoryFieldsFragment,
    DeprecationFieldsFragment,
    EmbedFieldsFragment,
    DataPlatformInstanceFieldsFragment,
    ParentContainersFieldsFragment,
    InputFieldsFieldsFragment,
    EntityContainerFragment,
    NonRecursiveMlFeatureTableFragment,
    NonRecursiveMlFeatureFragment,
    NonRecursiveMlPrimaryKeyFragment,
    SchemaMetadataFieldsFragment,
} from './fragments.generated';
import { gql } from '@apollo/client';
import {
    NonRecursiveDataFlowFieldsFragmentDoc,
    OwnershipFieldsFragmentDoc,
    ParentNodesFieldsFragmentDoc,
    GlossaryNodeFragmentDoc,
    PlatformFieldsFragmentDoc,
    GlobalTagsFieldsFragmentDoc,
    GlossaryTermsFragmentDoc,
    EntityDomainFragmentDoc,
    InstitutionalMemoryFieldsFragmentDoc,
    DeprecationFieldsFragmentDoc,
    EmbedFieldsFragmentDoc,
    DataPlatformInstanceFieldsFragmentDoc,
    ParentContainersFieldsFragmentDoc,
    InputFieldsFieldsFragmentDoc,
    EntityContainerFragmentDoc,
    NonRecursiveMlFeatureTableFragmentDoc,
    NonRecursiveMlFeatureFragmentDoc,
    NonRecursiveMlPrimaryKeyFragmentDoc,
    SchemaMetadataFieldsFragmentDoc,
} from './fragments.generated';
import * as Apollo from '@apollo/client';
export type LineageNodeProperties_Assertion_Fragment = { __typename?: 'Assertion' } & Pick<
    Types.Assertion,
    'urn' | 'type'
>;

export type LineageNodeProperties_Chart_Fragment = { __typename?: 'Chart' } & Pick<
    Types.Chart,
    'tool' | 'chartId' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'ChartProperties' } & Pick<Types.ChartProperties, 'name' | 'description'>
        >;
        editableProperties?: Types.Maybe<
            { __typename?: 'ChartEditableProperties' } & Pick<Types.ChartEditableProperties, 'description'>
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
    };

export type LineageNodeProperties_Dashboard_Fragment = { __typename?: 'Dashboard' } & Pick<
    Types.Dashboard,
    'urn' | 'type' | 'tool' | 'dashboardId'
> & {
        properties?: Types.Maybe<
            { __typename?: 'DashboardProperties' } & Pick<
                Types.DashboardProperties,
                'name' | 'description' | 'externalUrl' | 'lastRefreshed'
            > & {
                    created: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'>;
                    lastModified: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'>;
                }
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        parentContainers?: Types.Maybe<{ __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        editableProperties?: Types.Maybe<
            { __typename?: 'DashboardEditableProperties' } & Pick<Types.DashboardEditableProperties, 'description'>
        >;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
    };

export type LineageNodeProperties_DataFlow_Fragment = { __typename?: 'DataFlow' } & Pick<
    Types.DataFlow,
    'orchestrator' | 'flowId' | 'cluster' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'DataFlowProperties' } & Pick<Types.DataFlowProperties, 'name' | 'description' | 'project'>
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'DataFlowEditableProperties' } & Pick<Types.DataFlowEditableProperties, 'description'>
        >;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
    };

export type LineageNodeProperties_DataJob_Fragment = { __typename?: 'DataJob' } & Pick<
    Types.DataJob,
    'urn' | 'type' | 'jobId'
> & {
        dataFlow?: Types.Maybe<{ __typename?: 'DataFlow' } & NonRecursiveDataFlowFieldsFragment>;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        properties?: Types.Maybe<
            { __typename?: 'DataJobProperties' } & Pick<
                Types.DataJobProperties,
                'name' | 'description' | 'externalUrl'
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
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        editableProperties?: Types.Maybe<
            { __typename?: 'DataJobEditableProperties' } & Pick<Types.DataJobEditableProperties, 'description'>
        >;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
    };

export type LineageNodeProperties_DataProcessInstance_Fragment = { __typename?: 'DataProcessInstance' } & Pick<
    Types.DataProcessInstance,
    'urn' | 'type'
>;

export type LineageNodeProperties_Dataset_Fragment = { __typename?: 'Dataset' } & Pick<
    Types.Dataset,
    'name' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'DatasetProperties' } & Pick<
                Types.DatasetProperties,
                'name' | 'description' | 'qualifiedName'
            >
        >;
        editableProperties?: Types.Maybe<
            { __typename?: 'DatasetEditableProperties' } & Pick<Types.DatasetEditableProperties, 'description'>
        >;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
        fineGrainedLineages?: Types.Maybe<
            Array<
                { __typename?: 'FineGrainedLineage' } & {
                    upstreams?: Types.Maybe<
                        Array<{ __typename?: 'SchemaFieldRef' } & Pick<Types.SchemaFieldRef, 'urn' | 'path'>>
                    >;
                    downstreams?: Types.Maybe<
                        Array<{ __typename?: 'SchemaFieldRef' } & Pick<Types.SchemaFieldRef, 'urn' | 'path'>>
                    >;
                }
            >
        >;
    };

export type LineageNodeProperties_MlFeature_Fragment = { __typename?: 'MLFeature' } & Pick<
    Types.MlFeature,
    'urn' | 'type'
> &
    NonRecursiveMlFeatureFragment;

export type LineageNodeProperties_MlFeatureTable_Fragment = { __typename?: 'MLFeatureTable' } & Pick<
    Types.MlFeatureTable,
    'urn' | 'type'
> &
    NonRecursiveMlFeatureTableFragment;

export type LineageNodeProperties_MlModel_Fragment = { __typename?: 'MLModel' } & Pick<
    Types.MlModel,
    'urn' | 'type' | 'name' | 'description' | 'origin'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
    };

export type LineageNodeProperties_MlModelGroup_Fragment = { __typename?: 'MLModelGroup' } & Pick<
    Types.MlModelGroup,
    'urn' | 'type' | 'name' | 'description' | 'origin'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
    };

export type LineageNodeProperties_MlPrimaryKey_Fragment = { __typename?: 'MLPrimaryKey' } & Pick<
    Types.MlPrimaryKey,
    'urn' | 'type'
> &
    NonRecursiveMlPrimaryKeyFragment;

export type LineageNodePropertiesFragment =
    | LineageNodeProperties_Assertion_Fragment
    | LineageNodeProperties_Chart_Fragment
    | LineageNodeProperties_Dashboard_Fragment
    | LineageNodeProperties_DataFlow_Fragment
    | LineageNodeProperties_DataJob_Fragment
    | LineageNodeProperties_DataProcessInstance_Fragment
    | LineageNodeProperties_Dataset_Fragment
    | LineageNodeProperties_MlFeature_Fragment
    | LineageNodeProperties_MlFeatureTable_Fragment
    | LineageNodeProperties_MlModel_Fragment
    | LineageNodeProperties_MlModelGroup_Fragment
    | LineageNodeProperties_MlPrimaryKey_Fragment;

export type LineageFields_Assertion_Fragment = { __typename?: 'Assertion' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_Assertion_Fragment &
    CanEditLineageFragment_Assertion_Fragment;

export type LineageFields_Chart_Fragment = { __typename?: 'Chart' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_Chart_Fragment &
    CanEditLineageFragment_Chart_Fragment;

export type LineageFields_Dashboard_Fragment = { __typename?: 'Dashboard' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_Dashboard_Fragment &
    CanEditLineageFragment_Dashboard_Fragment;

export type LineageFields_DataFlow_Fragment = { __typename?: 'DataFlow' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_DataFlow_Fragment &
    CanEditLineageFragment_DataFlow_Fragment;

export type LineageFields_DataJob_Fragment = { __typename?: 'DataJob' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_DataJob_Fragment &
    CanEditLineageFragment_DataJob_Fragment;

export type LineageFields_DataProcessInstance_Fragment = { __typename?: 'DataProcessInstance' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_DataProcessInstance_Fragment &
    CanEditLineageFragment_DataProcessInstance_Fragment;

export type LineageFields_Dataset_Fragment = { __typename?: 'Dataset' } & {
    siblings?: Types.Maybe<
        { __typename?: 'SiblingProperties' } & Pick<Types.SiblingProperties, 'isPrimary'> & {
                siblings?: Types.Maybe<
                    Array<
                        Types.Maybe<
                            | ({ __typename?: 'AccessTokenMetadata' } & Pick<Types.AccessTokenMetadata, 'urn' | 'type'>)
                            | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'> &
                                  LineageNodeProperties_Assertion_Fragment)
                            | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'> &
                                  LineageNodeProperties_Chart_Fragment)
                            | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
                            | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
                            | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'>)
                            | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'> &
                                  LineageNodeProperties_Dashboard_Fragment)
                            | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'> &
                                  LineageNodeProperties_DataFlow_Fragment)
                            | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
                            | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
                            | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
                            | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'> &
                                  LineageNodeProperties_DataJob_Fragment)
                            | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
                            | ({ __typename?: 'DataPlatformInstance' } & Pick<
                                  Types.DataPlatformInstance,
                                  'urn' | 'type'
                              >)
                            | ({ __typename?: 'DataProcessInstance' } & Pick<
                                  Types.DataProcessInstance,
                                  'urn' | 'type'
                              > &
                                  LineageNodeProperties_DataProcessInstance_Fragment)
                            | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'> &
                                  LineageNodeProperties_Dataset_Fragment)
                            | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'>)
                            | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'>)
                            | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'>)
                            | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'> &
                                  LineageNodeProperties_MlFeature_Fragment)
                            | ({ __typename?: 'MLFeatureTable' } & Pick<Types.MlFeatureTable, 'urn' | 'type'> &
                                  LineageNodeProperties_MlFeatureTable_Fragment)
                            | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'> &
                                  LineageNodeProperties_MlModel_Fragment)
                            | ({ __typename?: 'MLModelGroup' } & Pick<Types.MlModelGroup, 'urn' | 'type'> &
                                  LineageNodeProperties_MlModelGroup_Fragment)
                            | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'> &
                                  LineageNodeProperties_MlPrimaryKey_Fragment)
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
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_Dataset_Fragment &
    CanEditLineageFragment_Dataset_Fragment;

export type LineageFields_MlFeature_Fragment = { __typename?: 'MLFeature' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_MlFeature_Fragment &
    CanEditLineageFragment_MlFeature_Fragment;

export type LineageFields_MlFeatureTable_Fragment = { __typename?: 'MLFeatureTable' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_MlFeatureTable_Fragment &
    CanEditLineageFragment_MlFeatureTable_Fragment;

export type LineageFields_MlModel_Fragment = { __typename?: 'MLModel' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_MlModel_Fragment &
    CanEditLineageFragment_MlModel_Fragment;

export type LineageFields_MlModelGroup_Fragment = { __typename?: 'MLModelGroup' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_MlModelGroup_Fragment &
    CanEditLineageFragment_MlModelGroup_Fragment;

export type LineageFields_MlPrimaryKey_Fragment = { __typename?: 'MLPrimaryKey' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_MlPrimaryKey_Fragment &
    CanEditLineageFragment_MlPrimaryKey_Fragment;

export type LineageFieldsFragment =
    | LineageFields_Assertion_Fragment
    | LineageFields_Chart_Fragment
    | LineageFields_Dashboard_Fragment
    | LineageFields_DataFlow_Fragment
    | LineageFields_DataJob_Fragment
    | LineageFields_DataProcessInstance_Fragment
    | LineageFields_Dataset_Fragment
    | LineageFields_MlFeature_Fragment
    | LineageFields_MlFeatureTable_Fragment
    | LineageFields_MlModel_Fragment
    | LineageFields_MlModelGroup_Fragment
    | LineageFields_MlPrimaryKey_Fragment;

export type LineageRelationshipFieldsFragment = { __typename?: 'LineageRelationship' } & Pick<
    Types.LineageRelationship,
    'type' | 'createdOn' | 'updatedOn' | 'isManual'
> & {
        createdActor?: Types.Maybe<
            | ({ __typename?: 'AccessTokenMetadata' } & Pick<Types.AccessTokenMetadata, 'urn' | 'type'>)
            | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'>)
            | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'>)
            | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
            | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
            | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'username' | 'urn' | 'type'> & {
                      info?: Types.Maybe<{ __typename?: 'CorpUserInfo' } & Pick<Types.CorpUserInfo, 'displayName'>>;
                      properties?: Types.Maybe<
                          { __typename?: 'CorpUserProperties' } & Pick<Types.CorpUserProperties, 'displayName'>
                      >;
                      editableProperties?: Types.Maybe<
                          { __typename?: 'CorpUserEditableProperties' } & Pick<
                              Types.CorpUserEditableProperties,
                              'displayName'
                          >
                      >;
                  })
            | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'>)
            | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'>)
            | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
            | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
            | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
            | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'>)
            | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
            | ({ __typename?: 'DataPlatformInstance' } & Pick<Types.DataPlatformInstance, 'urn' | 'type'>)
            | ({ __typename?: 'DataProcessInstance' } & Pick<Types.DataProcessInstance, 'urn' | 'type'>)
            | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'>)
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
        >;
        updatedActor?: Types.Maybe<
            | ({ __typename?: 'AccessTokenMetadata' } & Pick<Types.AccessTokenMetadata, 'urn' | 'type'>)
            | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'>)
            | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'>)
            | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
            | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
            | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'username' | 'urn' | 'type'> & {
                      info?: Types.Maybe<{ __typename?: 'CorpUserInfo' } & Pick<Types.CorpUserInfo, 'displayName'>>;
                      properties?: Types.Maybe<
                          { __typename?: 'CorpUserProperties' } & Pick<Types.CorpUserProperties, 'displayName'>
                      >;
                      editableProperties?: Types.Maybe<
                          { __typename?: 'CorpUserEditableProperties' } & Pick<
                              Types.CorpUserEditableProperties,
                              'displayName'
                          >
                      >;
                  })
            | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'>)
            | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'>)
            | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
            | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
            | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
            | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'>)
            | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
            | ({ __typename?: 'DataPlatformInstance' } & Pick<Types.DataPlatformInstance, 'urn' | 'type'>)
            | ({ __typename?: 'DataProcessInstance' } & Pick<Types.DataProcessInstance, 'urn' | 'type'>)
            | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'>)
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
        >;
    };

export type FullLineageResultsFragment = { __typename?: 'EntityLineageResult' } & Pick<
    Types.EntityLineageResult,
    'start' | 'count' | 'total' | 'filtered'
> & {
        relationships: Array<
            { __typename?: 'LineageRelationship' } & {
                entity?: Types.Maybe<
                    | { __typename?: 'AccessTokenMetadata' }
                    | ({ __typename?: 'Assertion' } & LineageFields_Assertion_Fragment)
                    | ({ __typename?: 'Chart' } & {
                          inputFields?: Types.Maybe<{ __typename?: 'InputFields' } & InputFieldsFieldsFragment>;
                      } & LineageFields_Chart_Fragment)
                    | { __typename?: 'Container' }
                    | { __typename?: 'CorpGroup' }
                    | { __typename?: 'CorpUser' }
                    | ({ __typename?: 'Dashboard' } & LineageFields_Dashboard_Fragment)
                    | ({ __typename?: 'DataFlow' } & LineageFields_DataFlow_Fragment)
                    | { __typename?: 'DataHubPolicy' }
                    | { __typename?: 'DataHubRole' }
                    | { __typename?: 'DataHubView' }
                    | ({ __typename?: 'DataJob' } & LineageFields_DataJob_Fragment)
                    | { __typename?: 'DataPlatform' }
                    | { __typename?: 'DataPlatformInstance' }
                    | ({ __typename?: 'DataProcessInstance' } & LineageFields_DataProcessInstance_Fragment)
                    | ({ __typename?: 'Dataset' } & {
                          schemaMetadata?: Types.Maybe<
                              { __typename?: 'SchemaMetadata' } & SchemaMetadataFieldsFragment
                          >;
                      } & LineageFields_Dataset_Fragment)
                    | { __typename?: 'Domain' }
                    | { __typename?: 'GlossaryNode' }
                    | { __typename?: 'GlossaryTerm' }
                    | ({ __typename?: 'MLFeature' } & LineageFields_MlFeature_Fragment)
                    | ({ __typename?: 'MLFeatureTable' } & LineageFields_MlFeatureTable_Fragment)
                    | ({ __typename?: 'MLModel' } & LineageFields_MlModel_Fragment)
                    | ({ __typename?: 'MLModelGroup' } & LineageFields_MlModelGroup_Fragment)
                    | ({ __typename?: 'MLPrimaryKey' } & LineageFields_MlPrimaryKey_Fragment)
                    | { __typename?: 'Notebook' }
                    | { __typename?: 'Post' }
                    | { __typename?: 'QueryEntity' }
                    | { __typename?: 'SchemaFieldEntity' }
                    | { __typename?: 'Tag' }
                    | { __typename?: 'Test' }
                    | { __typename?: 'VersionedDataset' }
                >;
            } & LineageRelationshipFieldsFragment
        >;
    };

export type LeafLineageResultsFragment = { __typename?: 'EntityLineageResult' } & Pick<
    Types.EntityLineageResult,
    'start' | 'count' | 'total' | 'filtered'
> & {
        relationships: Array<
            { __typename?: 'LineageRelationship' } & {
                entity?: Types.Maybe<
                    | ({ __typename?: 'AccessTokenMetadata' } & Pick<Types.AccessTokenMetadata, 'urn' | 'type'>)
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
                    | ({ __typename?: 'DataPlatformInstance' } & Pick<Types.DataPlatformInstance, 'urn' | 'type'>)
                    | ({ __typename?: 'DataProcessInstance' } & Pick<Types.DataProcessInstance, 'urn' | 'type'>)
                    | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'>)
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
                >;
            } & LineageRelationshipFieldsFragment
        >;
    };

export type PartialLineageResultsFragment = { __typename?: 'EntityLineageResult' } & Pick<
    Types.EntityLineageResult,
    'start' | 'count' | 'total' | 'filtered'
>;

export type GetEntityLineageQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    separateSiblings?: Types.Maybe<Types.Scalars['Boolean']>;
    showColumns: Types.Scalars['Boolean'];
    startTimeMillis?: Types.Maybe<Types.Scalars['Long']>;
    endTimeMillis?: Types.Maybe<Types.Scalars['Long']>;
    excludeUpstream?: Types.Maybe<Types.Scalars['Boolean']>;
    excludeDownstream?: Types.Maybe<Types.Scalars['Boolean']>;
}>;

export type GetEntityLineageQuery = { __typename?: 'Query' } & {
    entity?: Types.Maybe<
        | ({ __typename?: 'AccessTokenMetadata' } & Pick<Types.AccessTokenMetadata, 'urn' | 'type'>)
        | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_Assertion_Fragment &
              CanEditLineageFragment_Assertion_Fragment)
        | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'> & {
                  inputFields?: Types.Maybe<{ __typename?: 'InputFields' } & InputFieldsFieldsFragment>;
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_Chart_Fragment &
              CanEditLineageFragment_Chart_Fragment)
        | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
        | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
        | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'>)
        | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_Dashboard_Fragment &
              CanEditLineageFragment_Dashboard_Fragment)
        | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_DataFlow_Fragment &
              CanEditLineageFragment_DataFlow_Fragment)
        | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
        | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
        | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
        | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_DataJob_Fragment &
              CanEditLineageFragment_DataJob_Fragment)
        | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
        | ({ __typename?: 'DataPlatformInstance' } & Pick<Types.DataPlatformInstance, 'urn' | 'type'>)
        | ({ __typename?: 'DataProcessInstance' } & Pick<Types.DataProcessInstance, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_DataProcessInstance_Fragment &
              CanEditLineageFragment_DataProcessInstance_Fragment)
        | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'> & {
                  schemaMetadata?: Types.Maybe<{ __typename?: 'SchemaMetadata' } & SchemaMetadataFieldsFragment>;
                  siblings?: Types.Maybe<
                      { __typename?: 'SiblingProperties' } & Pick<Types.SiblingProperties, 'isPrimary'> & {
                              siblings?: Types.Maybe<
                                  Array<
                                      Types.Maybe<
                                          | ({ __typename?: 'AccessTokenMetadata' } & Pick<
                                                Types.AccessTokenMetadata,
                                                'urn' | 'type'
                                            >)
                                          | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'> &
                                                LineageNodeProperties_Assertion_Fragment)
                                          | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'> &
                                                LineageNodeProperties_Chart_Fragment)
                                          | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
                                          | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
                                          | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'>)
                                          | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'> &
                                                LineageNodeProperties_Dashboard_Fragment)
                                          | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'> &
                                                LineageNodeProperties_DataFlow_Fragment)
                                          | ({ __typename?: 'DataHubPolicy' } & Pick<
                                                Types.DataHubPolicy,
                                                'urn' | 'type'
                                            >)
                                          | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
                                          | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
                                          | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'> &
                                                LineageNodeProperties_DataJob_Fragment)
                                          | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
                                          | ({ __typename?: 'DataPlatformInstance' } & Pick<
                                                Types.DataPlatformInstance,
                                                'urn' | 'type'
                                            >)
                                          | ({ __typename?: 'DataProcessInstance' } & Pick<
                                                Types.DataProcessInstance,
                                                'urn' | 'type'
                                            > &
                                                LineageNodeProperties_DataProcessInstance_Fragment)
                                          | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'> &
                                                LineageNodeProperties_Dataset_Fragment)
                                          | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'>)
                                          | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'>)
                                          | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'>)
                                          | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'> &
                                                LineageNodeProperties_MlFeature_Fragment)
                                          | ({ __typename?: 'MLFeatureTable' } & Pick<
                                                Types.MlFeatureTable,
                                                'urn' | 'type'
                                            > &
                                                LineageNodeProperties_MlFeatureTable_Fragment)
                                          | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'> &
                                                LineageNodeProperties_MlModel_Fragment)
                                          | ({ __typename?: 'MLModelGroup' } & Pick<
                                                Types.MlModelGroup,
                                                'urn' | 'type'
                                            > &
                                                LineageNodeProperties_MlModelGroup_Fragment)
                                          | ({ __typename?: 'MLPrimaryKey' } & Pick<
                                                Types.MlPrimaryKey,
                                                'urn' | 'type'
                                            > &
                                                LineageNodeProperties_MlPrimaryKey_Fragment)
                                          | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>)
                                          | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>)
                                          | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'>)
                                          | ({ __typename?: 'SchemaFieldEntity' } & Pick<
                                                Types.SchemaFieldEntity,
                                                'urn' | 'type'
                                            >)
                                          | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type'>)
                                          | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>)
                                          | ({ __typename?: 'VersionedDataset' } & Pick<
                                                Types.VersionedDataset,
                                                'urn' | 'type'
                                            >)
                                      >
                                  >
                              >;
                          }
                  >;
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_Dataset_Fragment &
              CanEditLineageFragment_Dataset_Fragment)
        | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'>)
        | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'>)
        | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'>)
        | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_MlFeature_Fragment &
              CanEditLineageFragment_MlFeature_Fragment)
        | ({ __typename?: 'MLFeatureTable' } & Pick<Types.MlFeatureTable, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_MlFeatureTable_Fragment &
              CanEditLineageFragment_MlFeatureTable_Fragment)
        | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_MlModel_Fragment &
              CanEditLineageFragment_MlModel_Fragment)
        | ({ __typename?: 'MLModelGroup' } & Pick<Types.MlModelGroup, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_MlModelGroup_Fragment &
              CanEditLineageFragment_MlModelGroup_Fragment)
        | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
                  downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
              } & LineageNodeProperties_MlPrimaryKey_Fragment &
              CanEditLineageFragment_MlPrimaryKey_Fragment)
        | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>)
        | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>)
        | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'>)
        | ({ __typename?: 'SchemaFieldEntity' } & Pick<Types.SchemaFieldEntity, 'urn' | 'type'>)
        | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type'>)
        | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>)
        | ({ __typename?: 'VersionedDataset' } & Pick<Types.VersionedDataset, 'urn' | 'type'>)
    >;
};

export type CanEditLineageFragment_Assertion_Fragment = { __typename?: 'Assertion' };

export type CanEditLineageFragment_Chart_Fragment = { __typename?: 'Chart' } & {
    privileges?: Types.Maybe<{ __typename?: 'EntityPrivileges' } & Pick<Types.EntityPrivileges, 'canEditLineage'>>;
};

export type CanEditLineageFragment_Dashboard_Fragment = { __typename?: 'Dashboard' } & {
    privileges?: Types.Maybe<{ __typename?: 'EntityPrivileges' } & Pick<Types.EntityPrivileges, 'canEditLineage'>>;
};

export type CanEditLineageFragment_DataFlow_Fragment = { __typename?: 'DataFlow' };

export type CanEditLineageFragment_DataJob_Fragment = { __typename?: 'DataJob' } & {
    privileges?: Types.Maybe<{ __typename?: 'EntityPrivileges' } & Pick<Types.EntityPrivileges, 'canEditLineage'>>;
};

export type CanEditLineageFragment_DataProcessInstance_Fragment = { __typename?: 'DataProcessInstance' };

export type CanEditLineageFragment_Dataset_Fragment = { __typename?: 'Dataset' } & {
    privileges?: Types.Maybe<{ __typename?: 'EntityPrivileges' } & Pick<Types.EntityPrivileges, 'canEditLineage'>>;
};

export type CanEditLineageFragment_MlFeature_Fragment = { __typename?: 'MLFeature' };

export type CanEditLineageFragment_MlFeatureTable_Fragment = { __typename?: 'MLFeatureTable' };

export type CanEditLineageFragment_MlModel_Fragment = { __typename?: 'MLModel' };

export type CanEditLineageFragment_MlModelGroup_Fragment = { __typename?: 'MLModelGroup' };

export type CanEditLineageFragment_MlPrimaryKey_Fragment = { __typename?: 'MLPrimaryKey' };

export type CanEditLineageFragmentFragment =
    | CanEditLineageFragment_Assertion_Fragment
    | CanEditLineageFragment_Chart_Fragment
    | CanEditLineageFragment_Dashboard_Fragment
    | CanEditLineageFragment_DataFlow_Fragment
    | CanEditLineageFragment_DataJob_Fragment
    | CanEditLineageFragment_DataProcessInstance_Fragment
    | CanEditLineageFragment_Dataset_Fragment
    | CanEditLineageFragment_MlFeature_Fragment
    | CanEditLineageFragment_MlFeatureTable_Fragment
    | CanEditLineageFragment_MlModel_Fragment
    | CanEditLineageFragment_MlModelGroup_Fragment
    | CanEditLineageFragment_MlPrimaryKey_Fragment;

export type GetLineageCountsQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    separateSiblings?: Types.Maybe<Types.Scalars['Boolean']>;
    startTimeMillis?: Types.Maybe<Types.Scalars['Long']>;
    endTimeMillis?: Types.Maybe<Types.Scalars['Long']>;
    excludeUpstream?: Types.Maybe<Types.Scalars['Boolean']>;
    excludeDownstream?: Types.Maybe<Types.Scalars['Boolean']>;
}>;

export type GetLineageCountsQuery = { __typename?: 'Query' } & {
    entity?: Types.Maybe<
        | ({ __typename?: 'AccessTokenMetadata' } & Pick<Types.AccessTokenMetadata, 'urn' | 'type'>)
        | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
        | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
        | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'>)
        | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
        | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
        | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
        | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
        | ({ __typename?: 'DataPlatformInstance' } & Pick<Types.DataPlatformInstance, 'urn' | 'type'>)
        | ({ __typename?: 'DataProcessInstance' } & Pick<Types.DataProcessInstance, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'>)
        | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'>)
        | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'>)
        | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'MLFeatureTable' } & Pick<Types.MlFeatureTable, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'MLModelGroup' } & Pick<Types.MlModelGroup, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'> & {
                  upstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
                  downstream?: Types.Maybe<
                      { __typename?: 'EntityLineageResult' } & Pick<Types.EntityLineageResult, 'filtered' | 'total'>
                  >;
              })
        | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>)
        | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>)
        | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'>)
        | ({ __typename?: 'SchemaFieldEntity' } & Pick<Types.SchemaFieldEntity, 'urn' | 'type'>)
        | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type'>)
        | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>)
        | ({ __typename?: 'VersionedDataset' } & Pick<Types.VersionedDataset, 'urn' | 'type'>)
    >;
};

export const LineageRelationshipFieldsFragmentDoc = gql`
    fragment lineageRelationshipFields on LineageRelationship {
        type
        createdOn
        createdActor {
            urn
            type
            ... on CorpUser {
                username
                info {
                    displayName
                }
                properties {
                    displayName
                }
                editableProperties {
                    displayName
                }
            }
        }
        updatedOn
        updatedActor {
            urn
            type
            ... on CorpUser {
                username
                info {
                    displayName
                }
                properties {
                    displayName
                }
                editableProperties {
                    displayName
                }
            }
        }
        isManual
    }
`;
export const LineageNodePropertiesFragmentDoc = gql`
    fragment lineageNodeProperties on EntityWithRelationships {
        urn
        type
        ... on DataJob {
            urn
            type
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
                customProperties {
                    key
                    value
                }
            }
            globalTags {
                ...globalTagsFields
            }
            glossaryTerms {
                ...glossaryTerms
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
            editableProperties {
                description
            }
            status {
                removed
            }
        }
        ... on DataFlow {
            orchestrator
            flowId
            cluster
            properties {
                name
                description
                project
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
            domain {
                ...entityDomain
            }
            status {
                removed
            }
        }
        ... on Dashboard {
            urn
            type
            tool
            dashboardId
            properties {
                name
                description
                externalUrl
                lastRefreshed
                created {
                    time
                }
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
            platform {
                ...platformFields
            }
            domain {
                ...entityDomain
            }
            parentContainers {
                ...parentContainersFields
            }
            status {
                removed
            }
            deprecation {
                ...deprecationFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            editableProperties {
                description
            }
            status {
                removed
            }
            subTypes {
                typeNames
            }
        }
        ... on Chart {
            tool
            chartId
            properties {
                name
                description
            }
            editableProperties {
                description
            }
            ownership {
                ...ownershipFields
            }
            platform {
                ...platformFields
            }
            domain {
                ...entityDomain
            }
            status {
                removed
            }
        }
        ... on Dataset {
            name
            properties {
                name
                description
                qualifiedName
            }
            editableProperties {
                description
            }
            platform {
                ...platformFields
            }
            ownership {
                ...ownershipFields
            }
            subTypes {
                typeNames
            }
            status {
                removed
            }
            fineGrainedLineages {
                upstreams {
                    urn
                    path
                }
                downstreams {
                    urn
                    path
                }
            }
        }
        ... on MLModelGroup {
            urn
            type
            name
            description
            origin
            platform {
                ...platformFields
            }
            ownership {
                ...ownershipFields
            }
            status {
                removed
            }
        }
        ... on MLModel {
            urn
            type
            name
            description
            origin
            platform {
                ...platformFields
            }
            ownership {
                ...ownershipFields
            }
            status {
                removed
            }
        }
        ... on MLFeatureTable {
            ...nonRecursiveMLFeatureTable
        }
        ... on MLFeature {
            ...nonRecursiveMLFeature
        }
        ... on MLPrimaryKey {
            ...nonRecursiveMLPrimaryKey
        }
    }
    ${NonRecursiveDataFlowFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
    ${PlatformFieldsFragmentDoc}
    ${ParentContainersFieldsFragmentDoc}
    ${NonRecursiveMlFeatureTableFragmentDoc}
    ${NonRecursiveMlFeatureFragmentDoc}
    ${NonRecursiveMlPrimaryKeyFragmentDoc}
`;
export const CanEditLineageFragmentFragmentDoc = gql`
    fragment canEditLineageFragment on EntityWithRelationships {
        ... on Dataset {
            privileges {
                canEditLineage
            }
        }
        ... on Chart {
            privileges {
                canEditLineage
            }
        }
        ... on Dashboard {
            privileges {
                canEditLineage
            }
        }
        ... on DataJob {
            privileges {
                canEditLineage
            }
        }
    }
`;
export const LeafLineageResultsFragmentDoc = gql`
    fragment leafLineageResults on EntityLineageResult {
        start
        count
        total
        filtered
        relationships {
            ...lineageRelationshipFields
            entity {
                urn
                type
            }
        }
    }
    ${LineageRelationshipFieldsFragmentDoc}
`;
export const LineageFieldsFragmentDoc = gql`
    fragment lineageFields on EntityWithRelationships {
        ...lineageNodeProperties
        ...canEditLineageFragment
        ... on Dataset {
            siblings {
                isPrimary
                siblings {
                    urn
                    type
                    ...lineageNodeProperties
                }
            }
        }
        upstream: lineage(input: { direction: UPSTREAM, start: 0, count: 100, separateSiblings: $separateSiblings }) {
            ...leafLineageResults
        }
        downstream: lineage(
            input: { direction: DOWNSTREAM, start: 0, count: 100, separateSiblings: $separateSiblings }
        ) {
            ...leafLineageResults
        }
    }
    ${LineageNodePropertiesFragmentDoc}
    ${CanEditLineageFragmentFragmentDoc}
    ${LeafLineageResultsFragmentDoc}
`;
export const FullLineageResultsFragmentDoc = gql`
    fragment fullLineageResults on EntityLineageResult {
        start
        count
        total
        filtered
        relationships {
            ...lineageRelationshipFields
            entity {
                ...lineageFields
                ... on Dataset {
                    schemaMetadata(version: 0) @include(if: $showColumns) {
                        ...schemaMetadataFields
                    }
                }
                ... on Chart {
                    inputFields @include(if: $showColumns) {
                        ...inputFieldsFields
                    }
                }
            }
        }
    }
    ${LineageRelationshipFieldsFragmentDoc}
    ${LineageFieldsFragmentDoc}
    ${SchemaMetadataFieldsFragmentDoc}
    ${InputFieldsFieldsFragmentDoc}
`;
export const PartialLineageResultsFragmentDoc = gql`
    fragment partialLineageResults on EntityLineageResult {
        start
        count
        total
        filtered
    }
`;
export const GetEntityLineageDocument = gql`
    query getEntityLineage(
        $urn: String!
        $separateSiblings: Boolean
        $showColumns: Boolean!
        $startTimeMillis: Long
        $endTimeMillis: Long
        $excludeUpstream: Boolean = false
        $excludeDownstream: Boolean = false
    ) {
        entity(urn: $urn) {
            urn
            type
            ...lineageNodeProperties
            ...canEditLineageFragment
            ... on Dataset {
                schemaMetadata(version: 0) @include(if: $showColumns) {
                    ...schemaMetadataFields
                }
                siblings {
                    isPrimary
                    siblings {
                        urn
                        type
                        ...lineageNodeProperties
                    }
                }
            }
            ... on Chart {
                inputFields @include(if: $showColumns) {
                    ...inputFieldsFields
                }
            }
            ... on EntityWithRelationships {
                upstream: lineage(
                    input: {
                        direction: UPSTREAM
                        start: 0
                        count: 100
                        separateSiblings: $separateSiblings
                        startTimeMillis: $startTimeMillis
                        endTimeMillis: $endTimeMillis
                    }
                ) @skip(if: $excludeUpstream) {
                    ...fullLineageResults
                }
                downstream: lineage(
                    input: {
                        direction: DOWNSTREAM
                        start: 0
                        count: 100
                        separateSiblings: $separateSiblings
                        startTimeMillis: $startTimeMillis
                        endTimeMillis: $endTimeMillis
                    }
                ) @skip(if: $excludeDownstream) {
                    ...fullLineageResults
                }
            }
        }
    }
    ${LineageNodePropertiesFragmentDoc}
    ${CanEditLineageFragmentFragmentDoc}
    ${SchemaMetadataFieldsFragmentDoc}
    ${InputFieldsFieldsFragmentDoc}
    ${FullLineageResultsFragmentDoc}
`;

/**
 * __useGetEntityLineageQuery__
 *
 * To run a query within a React component, call `useGetEntityLineageQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetEntityLineageQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetEntityLineageQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *      separateSiblings: // value for 'separateSiblings'
 *      showColumns: // value for 'showColumns'
 *      startTimeMillis: // value for 'startTimeMillis'
 *      endTimeMillis: // value for 'endTimeMillis'
 *      excludeUpstream: // value for 'excludeUpstream'
 *      excludeDownstream: // value for 'excludeDownstream'
 *   },
 * });
 */
export function useGetEntityLineageQuery(
    baseOptions: Apollo.QueryHookOptions<GetEntityLineageQuery, GetEntityLineageQueryVariables>,
) {
    return Apollo.useQuery<GetEntityLineageQuery, GetEntityLineageQueryVariables>(
        GetEntityLineageDocument,
        baseOptions,
    );
}
export function useGetEntityLineageLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetEntityLineageQuery, GetEntityLineageQueryVariables>,
) {
    return Apollo.useLazyQuery<GetEntityLineageQuery, GetEntityLineageQueryVariables>(
        GetEntityLineageDocument,
        baseOptions,
    );
}
export type GetEntityLineageQueryHookResult = ReturnType<typeof useGetEntityLineageQuery>;
export type GetEntityLineageLazyQueryHookResult = ReturnType<typeof useGetEntityLineageLazyQuery>;
export type GetEntityLineageQueryResult = Apollo.QueryResult<GetEntityLineageQuery, GetEntityLineageQueryVariables>;
export const GetLineageCountsDocument = gql`
    query getLineageCounts(
        $urn: String!
        $separateSiblings: Boolean
        $startTimeMillis: Long
        $endTimeMillis: Long
        $excludeUpstream: Boolean = false
        $excludeDownstream: Boolean = false
    ) {
        entity(urn: $urn) {
            urn
            type
            ... on EntityWithRelationships {
                upstream: lineage(
                    input: {
                        direction: UPSTREAM
                        start: 0
                        count: 100
                        separateSiblings: $separateSiblings
                        startTimeMillis: $startTimeMillis
                        endTimeMillis: $endTimeMillis
                    }
                ) @skip(if: $excludeUpstream) {
                    filtered
                    total
                }
                downstream: lineage(
                    input: {
                        direction: DOWNSTREAM
                        start: 0
                        count: 100
                        separateSiblings: $separateSiblings
                        startTimeMillis: $startTimeMillis
                        endTimeMillis: $endTimeMillis
                    }
                ) @skip(if: $excludeDownstream) {
                    filtered
                    total
                }
            }
        }
    }
`;

/**
 * __useGetLineageCountsQuery__
 *
 * To run a query within a React component, call `useGetLineageCountsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetLineageCountsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetLineageCountsQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *      separateSiblings: // value for 'separateSiblings'
 *      startTimeMillis: // value for 'startTimeMillis'
 *      endTimeMillis: // value for 'endTimeMillis'
 *      excludeUpstream: // value for 'excludeUpstream'
 *      excludeDownstream: // value for 'excludeDownstream'
 *   },
 * });
 */
export function useGetLineageCountsQuery(
    baseOptions: Apollo.QueryHookOptions<GetLineageCountsQuery, GetLineageCountsQueryVariables>,
) {
    return Apollo.useQuery<GetLineageCountsQuery, GetLineageCountsQueryVariables>(
        GetLineageCountsDocument,
        baseOptions,
    );
}
export function useGetLineageCountsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetLineageCountsQuery, GetLineageCountsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetLineageCountsQuery, GetLineageCountsQueryVariables>(
        GetLineageCountsDocument,
        baseOptions,
    );
}
export type GetLineageCountsQueryHookResult = ReturnType<typeof useGetLineageCountsQuery>;
export type GetLineageCountsLazyQueryHookResult = ReturnType<typeof useGetLineageCountsLazyQuery>;
export type GetLineageCountsQueryResult = Apollo.QueryResult<GetLineageCountsQuery, GetLineageCountsQueryVariables>;
