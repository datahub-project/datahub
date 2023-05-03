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
export type EntityPreview_AccessTokenMetadata_Fragment = { __typename?: 'AccessTokenMetadata' } & Pick<
    Types.AccessTokenMetadata,
    'urn' | 'type'
>;

export type EntityPreview_Assertion_Fragment = { __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'>;

export type EntityPreview_Chart_Fragment = { __typename?: 'Chart' } & Pick<
    Types.Chart,
    'urn' | 'type' | 'tool' | 'chartId'
> & {
        properties?: Types.Maybe<
            { __typename?: 'ChartProperties' } & Pick<
                Types.ChartProperties,
                'name' | 'description' | 'externalUrl' | 'type' | 'access'
            > & { lastModified: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'> }
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'ChartEditableProperties' } & Pick<Types.ChartEditableProperties, 'description'>
        >;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
    };

export type EntityPreview_Container_Fragment = { __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'> &
    EntityContainerFragment;

export type EntityPreview_CorpGroup_Fragment = { __typename?: 'CorpGroup' } & Pick<
    Types.CorpGroup,
    'name' | 'urn' | 'type'
> & {
        info?: Types.Maybe<{ __typename?: 'CorpGroupInfo' } & Pick<Types.CorpGroupInfo, 'displayName' | 'description'>>;
        memberCount?: Types.Maybe<
            { __typename?: 'EntityRelationshipsResult' } & Pick<Types.EntityRelationshipsResult, 'total'>
        >;
    };

export type EntityPreview_CorpUser_Fragment = { __typename?: 'CorpUser' } & Pick<
    Types.CorpUser,
    'username' | 'urn' | 'type'
> & {
        info?: Types.Maybe<
            { __typename?: 'CorpUserInfo' } & Pick<
                Types.CorpUserInfo,
                'active' | 'displayName' | 'title' | 'firstName' | 'lastName' | 'fullName'
            >
        >;
        editableProperties?: Types.Maybe<
            { __typename?: 'CorpUserEditableProperties' } & Pick<
                Types.CorpUserEditableProperties,
                'displayName' | 'title' | 'pictureLink'
            >
        >;
    };

export type EntityPreview_Dashboard_Fragment = { __typename?: 'Dashboard' } & Pick<
    Types.Dashboard,
    'urn' | 'type' | 'tool' | 'dashboardId'
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
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
    };

export type EntityPreview_DataFlow_Fragment = { __typename?: 'DataFlow' } & Pick<
    Types.DataFlow,
    'urn' | 'type' | 'orchestrator' | 'flowId' | 'cluster'
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
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
    };

export type EntityPreview_DataHubPolicy_Fragment = { __typename?: 'DataHubPolicy' } & Pick<
    Types.DataHubPolicy,
    'urn' | 'type'
>;

export type EntityPreview_DataHubRole_Fragment = { __typename?: 'DataHubRole' } & Pick<
    Types.DataHubRole,
    'urn' | 'type'
>;

export type EntityPreview_DataHubView_Fragment = { __typename?: 'DataHubView' } & Pick<
    Types.DataHubView,
    'urn' | 'type'
>;

export type EntityPreview_DataJob_Fragment = { __typename?: 'DataJob' } & Pick<
    Types.DataJob,
    'urn' | 'type' | 'jobId'
> & {
        dataFlow?: Types.Maybe<{ __typename?: 'DataFlow' } & NonRecursiveDataFlowFieldsFragment>;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        properties?: Types.Maybe<
            { __typename?: 'DataJobProperties' } & Pick<Types.DataJobProperties, 'name' | 'description'>
        >;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'DataJobEditableProperties' } & Pick<Types.DataJobEditableProperties, 'description'>
        >;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
    };

export type EntityPreview_DataPlatform_Fragment = { __typename?: 'DataPlatform' } & Pick<
    Types.DataPlatform,
    'urn' | 'type'
> &
    NonConflictingPlatformFieldsFragment;

export type EntityPreview_DataPlatformInstance_Fragment = { __typename?: 'DataPlatformInstance' } & Pick<
    Types.DataPlatformInstance,
    'urn' | 'type'
>;

export type EntityPreview_DataProcessInstance_Fragment = { __typename?: 'DataProcessInstance' } & Pick<
    Types.DataProcessInstance,
    'urn' | 'type'
>;

export type EntityPreview_Dataset_Fragment = { __typename?: 'Dataset' } & Pick<
    Types.Dataset,
    'name' | 'origin' | 'uri' | 'platformNativeType' | 'urn' | 'type'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        editableProperties?: Types.Maybe<
            { __typename?: 'DatasetEditableProperties' } & Pick<Types.DatasetEditableProperties, 'description'>
        >;
        properties?: Types.Maybe<
            { __typename?: 'DatasetProperties' } & Pick<Types.DatasetProperties, 'name' | 'description'> & {
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
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
    };

export type EntityPreview_Domain_Fragment = { __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'> & {
        properties?: Types.Maybe<{ __typename?: 'DomainProperties' } & Pick<Types.DomainProperties, 'name'>>;
    };

export type EntityPreview_GlossaryNode_Fragment = { __typename?: 'GlossaryNode' } & Pick<
    Types.GlossaryNode,
    'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'GlossaryNodeProperties' } & Pick<Types.GlossaryNodeProperties, 'name' | 'description'>
        >;
    };

export type EntityPreview_GlossaryTerm_Fragment = { __typename?: 'GlossaryTerm' } & Pick<
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
    };

export type EntityPreview_MlFeature_Fragment = { __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'>;

export type EntityPreview_MlFeatureTable_Fragment = { __typename?: 'MLFeatureTable' } & Pick<
    Types.MlFeatureTable,
    'urn' | 'type' | 'name' | 'description'
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
    };

export type EntityPreview_MlModel_Fragment = { __typename?: 'MLModel' } & Pick<
    Types.MlModel,
    'name' | 'description' | 'origin' | 'urn' | 'type'
> & {
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
    };

export type EntityPreview_MlModelGroup_Fragment = { __typename?: 'MLModelGroup' } & Pick<
    Types.MlModelGroup,
    'name' | 'origin' | 'description' | 'urn' | 'type'
> & {
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
    };

export type EntityPreview_MlPrimaryKey_Fragment = { __typename?: 'MLPrimaryKey' } & Pick<
    Types.MlPrimaryKey,
    'urn' | 'type'
>;

export type EntityPreview_Notebook_Fragment = { __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>;

export type EntityPreview_Post_Fragment = { __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>;

export type EntityPreview_QueryEntity_Fragment = { __typename?: 'QueryEntity' } & Pick<
    Types.QueryEntity,
    'urn' | 'type'
>;

export type EntityPreview_SchemaFieldEntity_Fragment = { __typename?: 'SchemaFieldEntity' } & Pick<
    Types.SchemaFieldEntity,
    'urn' | 'type'
>;

export type EntityPreview_Tag_Fragment = { __typename?: 'Tag' } & Pick<
    Types.Tag,
    'name' | 'description' | 'urn' | 'type'
> & {
        properties?: Types.Maybe<
            { __typename?: 'TagProperties' } & Pick<Types.TagProperties, 'name' | 'description' | 'colorHex'>
        >;
    };

export type EntityPreview_Test_Fragment = { __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>;

export type EntityPreview_VersionedDataset_Fragment = { __typename?: 'VersionedDataset' } & Pick<
    Types.VersionedDataset,
    'urn' | 'type'
>;

export type EntityPreviewFragment =
    | EntityPreview_AccessTokenMetadata_Fragment
    | EntityPreview_Assertion_Fragment
    | EntityPreview_Chart_Fragment
    | EntityPreview_Container_Fragment
    | EntityPreview_CorpGroup_Fragment
    | EntityPreview_CorpUser_Fragment
    | EntityPreview_Dashboard_Fragment
    | EntityPreview_DataFlow_Fragment
    | EntityPreview_DataHubPolicy_Fragment
    | EntityPreview_DataHubRole_Fragment
    | EntityPreview_DataHubView_Fragment
    | EntityPreview_DataJob_Fragment
    | EntityPreview_DataPlatform_Fragment
    | EntityPreview_DataPlatformInstance_Fragment
    | EntityPreview_DataProcessInstance_Fragment
    | EntityPreview_Dataset_Fragment
    | EntityPreview_Domain_Fragment
    | EntityPreview_GlossaryNode_Fragment
    | EntityPreview_GlossaryTerm_Fragment
    | EntityPreview_MlFeature_Fragment
    | EntityPreview_MlFeatureTable_Fragment
    | EntityPreview_MlModel_Fragment
    | EntityPreview_MlModelGroup_Fragment
    | EntityPreview_MlPrimaryKey_Fragment
    | EntityPreview_Notebook_Fragment
    | EntityPreview_Post_Fragment
    | EntityPreview_QueryEntity_Fragment
    | EntityPreview_SchemaFieldEntity_Fragment
    | EntityPreview_Tag_Fragment
    | EntityPreview_Test_Fragment
    | EntityPreview_VersionedDataset_Fragment;

export const EntityPreviewFragmentDoc = gql`
    fragment entityPreview on Entity {
        urn
        type
        ... on Dataset {
            name
            origin
            uri
            platform {
                ...platformFields
            }
            editableProperties {
                description
            }
            platformNativeType
            properties {
                name
                description
                customProperties {
                    key
                    value
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
            subTypes {
                typeNames
            }
            domain {
                ...entityDomain
            }
            deprecation {
                ...deprecationFields
            }
        }
        ... on CorpUser {
            username
            info {
                active
                displayName
                title
                firstName
                lastName
                fullName
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
            urn
            type
            tool
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
            domain {
                ...entityDomain
            }
            deprecation {
                ...deprecationFields
            }
            subTypes {
                typeNames
            }
        }
        ... on Chart {
            urn
            type
            tool
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
            deprecation {
                ...deprecationFields
            }
        }
        ... on DataFlow {
            urn
            type
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
            deprecation {
                ...deprecationFields
            }
        }
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
        }
        ... on GlossaryNode {
            properties {
                name
                description
            }
        }
        ... on MLFeatureTable {
            urn
            type
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
        }
        ... on Tag {
            name
            description
            properties {
                name
                description
                colorHex
            }
        }
        ... on DataPlatform {
            ...nonConflictingPlatformFields
        }
        ... on Domain {
            urn
            properties {
                name
            }
        }
        ... on Container {
            ...entityContainer
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${NonRecursiveDataFlowFieldsFragmentDoc}
    ${NonConflictingPlatformFieldsFragmentDoc}
    ${EntityContainerFragmentDoc}
`;
