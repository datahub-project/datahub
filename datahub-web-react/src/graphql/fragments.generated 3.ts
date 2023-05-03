/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
export type GlobalTagsFieldsFragment = { __typename?: 'GlobalTags' } & {
    tags?: Types.Maybe<
        Array<
            { __typename?: 'TagAssociation' } & Pick<Types.TagAssociation, 'associatedUrn'> & {
                    tag: { __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type' | 'name' | 'description'> & {
                            properties?: Types.Maybe<
                                { __typename?: 'TagProperties' } & Pick<Types.TagProperties, 'name' | 'colorHex'>
                            >;
                        };
                }
        >
    >;
};

export type GlossaryNodeFragment = { __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'> & {
        properties?: Types.Maybe<
            { __typename?: 'GlossaryNodeProperties' } & Pick<Types.GlossaryNodeProperties, 'name'>
        >;
        children?: Types.Maybe<
            { __typename?: 'EntityRelationshipsResult' } & Pick<Types.EntityRelationshipsResult, 'total'>
        >;
    };

export type GlossaryTermFragment = { __typename?: 'GlossaryTerm' } & Pick<
    Types.GlossaryTerm,
    'urn' | 'name' | 'type' | 'hierarchicalName'
> & {
        properties?: Types.Maybe<
            { __typename?: 'GlossaryTermProperties' } & Pick<
                Types.GlossaryTermProperties,
                'name' | 'description' | 'definition' | 'termSource'
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
        parentNodes?: Types.Maybe<{ __typename?: 'ParentNodesResult' } & ParentNodesFieldsFragment>;
    };

export type GlossaryTermsFragment = { __typename?: 'GlossaryTerms' } & {
    terms?: Types.Maybe<
        Array<
            { __typename?: 'GlossaryTermAssociation' } & Pick<Types.GlossaryTermAssociation, 'associatedUrn'> & {
                    term: { __typename?: 'GlossaryTerm' } & GlossaryTermFragment;
                }
        >
    >;
};

export type DeprecationFieldsFragment = { __typename?: 'Deprecation' } & Pick<
    Types.Deprecation,
    'actor' | 'deprecated' | 'note' | 'decommissionTime'
>;

export type ParentContainersFieldsFragment = { __typename?: 'ParentContainersResult' } & Pick<
    Types.ParentContainersResult,
    'count'
> & { containers: Array<{ __typename?: 'Container' } & ParentContainerFieldsFragment> };

export type ParentNodesFieldsFragment = { __typename?: 'ParentNodesResult' } & Pick<
    Types.ParentNodesResult,
    'count'
> & {
        nodes: Array<
            { __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'> & {
                    properties?: Types.Maybe<
                        { __typename?: 'GlossaryNodeProperties' } & Pick<Types.GlossaryNodeProperties, 'name'>
                    >;
                }
        >;
    };

export type OwnershipFieldsFragment = { __typename?: 'Ownership' } & {
    owners?: Types.Maybe<
        Array<
            { __typename?: 'Owner' } & Pick<Types.Owner, 'type' | 'associatedUrn'> & {
                    owner:
                        | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type' | 'username'> & {
                                  info?: Types.Maybe<
                                      { __typename?: 'CorpUserInfo' } & Pick<
                                          Types.CorpUserInfo,
                                          | 'active'
                                          | 'displayName'
                                          | 'title'
                                          | 'email'
                                          | 'firstName'
                                          | 'lastName'
                                          | 'fullName'
                                      >
                                  >;
                                  properties?: Types.Maybe<
                                      { __typename?: 'CorpUserProperties' } & Pick<
                                          Types.CorpUserProperties,
                                          | 'active'
                                          | 'displayName'
                                          | 'title'
                                          | 'email'
                                          | 'firstName'
                                          | 'lastName'
                                          | 'fullName'
                                      >
                                  >;
                                  editableProperties?: Types.Maybe<
                                      { __typename?: 'CorpUserEditableProperties' } & Pick<
                                          Types.CorpUserEditableProperties,
                                          'displayName' | 'title' | 'pictureLink' | 'email'
                                      >
                                  >;
                              })
                        | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type' | 'name'> & {
                                  properties?: Types.Maybe<
                                      { __typename?: 'CorpGroupProperties' } & Pick<
                                          Types.CorpGroupProperties,
                                          'displayName' | 'email'
                                      >
                                  >;
                                  info?: Types.Maybe<
                                      { __typename?: 'CorpGroupInfo' } & Pick<
                                          Types.CorpGroupInfo,
                                          'displayName' | 'email' | 'groups'
                                      > & {
                                              admins?: Types.Maybe<
                                                  Array<
                                                      { __typename?: 'CorpUser' } & Pick<
                                                          Types.CorpUser,
                                                          'urn' | 'username'
                                                      > & {
                                                              info?: Types.Maybe<
                                                                  { __typename?: 'CorpUserInfo' } & Pick<
                                                                      Types.CorpUserInfo,
                                                                      | 'active'
                                                                      | 'displayName'
                                                                      | 'title'
                                                                      | 'email'
                                                                      | 'firstName'
                                                                      | 'lastName'
                                                                      | 'fullName'
                                                                  >
                                                              >;
                                                              editableInfo?: Types.Maybe<
                                                                  { __typename?: 'CorpUserEditableInfo' } & Pick<
                                                                      Types.CorpUserEditableInfo,
                                                                      'pictureLink' | 'teams' | 'skills'
                                                                  >
                                                              >;
                                                          }
                                                  >
                                              >;
                                              members?: Types.Maybe<
                                                  Array<
                                                      { __typename?: 'CorpUser' } & Pick<
                                                          Types.CorpUser,
                                                          'urn' | 'username'
                                                      > & {
                                                              info?: Types.Maybe<
                                                                  { __typename?: 'CorpUserInfo' } & Pick<
                                                                      Types.CorpUserInfo,
                                                                      | 'active'
                                                                      | 'displayName'
                                                                      | 'title'
                                                                      | 'email'
                                                                      | 'firstName'
                                                                      | 'lastName'
                                                                      | 'fullName'
                                                                  >
                                                              >;
                                                              editableInfo?: Types.Maybe<
                                                                  { __typename?: 'CorpUserEditableInfo' } & Pick<
                                                                      Types.CorpUserEditableInfo,
                                                                      'pictureLink' | 'teams' | 'skills'
                                                                  >
                                                              >;
                                                          }
                                                  >
                                              >;
                                          }
                                  >;
                              });
                }
        >
    >;
    lastModified: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'>;
};

export type EmbedFieldsFragment = { __typename?: 'Embed' } & Pick<Types.Embed, 'renderUrl'>;

export type InstitutionalMemoryFieldsFragment = { __typename?: 'InstitutionalMemory' } & {
    elements: Array<
        { __typename?: 'InstitutionalMemoryMetadata' } & Pick<
            Types.InstitutionalMemoryMetadata,
            'url' | 'description'
        > & {
                author: { __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'username'>;
                created: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'actor' | 'time'>;
            }
    >;
};

export type NonRecursiveDatasetFieldsFragment = { __typename?: 'Dataset' } & Pick<
    Types.Dataset,
    'urn' | 'name' | 'type' | 'origin' | 'uri' | 'lastIngested' | 'platformNativeType'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        properties?: Types.Maybe<
            { __typename?: 'DatasetProperties' } & Pick<
                Types.DatasetProperties,
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
        editableProperties?: Types.Maybe<
            { __typename?: 'DatasetEditableProperties' } & Pick<Types.DatasetEditableProperties, 'description'>
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        institutionalMemory?: Types.Maybe<{ __typename?: 'InstitutionalMemory' } & InstitutionalMemoryFieldsFragment>;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        container?: Types.Maybe<{ __typename?: 'Container' } & EntityContainerFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        embed?: Types.Maybe<{ __typename?: 'Embed' } & EmbedFieldsFragment>;
    };

export type NonRecursiveDataFlowFieldsFragment = { __typename?: 'DataFlow' } & Pick<
    Types.DataFlow,
    'urn' | 'type' | 'orchestrator' | 'flowId' | 'cluster'
> & {
        properties?: Types.Maybe<
            { __typename?: 'DataFlowProperties' } & Pick<
                Types.DataFlowProperties,
                'name' | 'description' | 'project' | 'externalUrl'
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
        editableProperties?: Types.Maybe<
            { __typename?: 'DataFlowEditableProperties' } & Pick<Types.DataFlowEditableProperties, 'description'>
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
    };

export type NonRecursiveDataJobFieldsFragment = { __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn'> & {
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
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
    };

export type DataJobFieldsFragment = { __typename?: 'DataJob' } & Pick<
    Types.DataJob,
    'urn' | 'type' | 'exists' | 'lastIngested' | 'jobId'
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
        editableProperties?: Types.Maybe<
            { __typename?: 'DataJobEditableProperties' } & Pick<Types.DataJobEditableProperties, 'description'>
        >;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        institutionalMemory?: Types.Maybe<{ __typename?: 'InstitutionalMemory' } & InstitutionalMemoryFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        privileges?: Types.Maybe<{ __typename?: 'EntityPrivileges' } & Pick<Types.EntityPrivileges, 'canEditLineage'>>;
    };

export type DashboardFieldsFragment = { __typename?: 'Dashboard' } & Pick<
    Types.Dashboard,
    'urn' | 'type' | 'exists' | 'lastIngested' | 'tool' | 'dashboardId'
> & {
        properties?: Types.Maybe<
            { __typename?: 'DashboardProperties' } & Pick<
                Types.DashboardProperties,
                'name' | 'description' | 'externalUrl' | 'access' | 'lastRefreshed'
            > & {
                    customProperties?: Types.Maybe<
                        Array<
                            { __typename?: 'CustomPropertiesEntry' } & Pick<
                                Types.CustomPropertiesEntry,
                                'key' | 'value'
                            >
                        >
                    >;
                    created: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'>;
                    lastModified: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'>;
                }
        >;
        editableProperties?: Types.Maybe<
            { __typename?: 'DashboardEditableProperties' } & Pick<Types.DashboardEditableProperties, 'description'>
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        institutionalMemory?: Types.Maybe<{ __typename?: 'InstitutionalMemory' } & InstitutionalMemoryFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        parentContainers?: Types.Maybe<{ __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
        embed?: Types.Maybe<{ __typename?: 'Embed' } & EmbedFieldsFragment>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
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
        inputFields?: Types.Maybe<{ __typename?: 'InputFields' } & InputFieldsFieldsFragment>;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
        privileges?: Types.Maybe<
            { __typename?: 'EntityPrivileges' } & Pick<Types.EntityPrivileges, 'canEditLineage' | 'canEditEmbed'>
        >;
    };

export type NonRecursiveMlFeatureFragment = { __typename?: 'MLFeature' } & Pick<
    Types.MlFeature,
    'urn' | 'type' | 'exists' | 'lastIngested' | 'name' | 'featureNamespace' | 'description' | 'dataType'
> & {
        properties?: Types.Maybe<
            { __typename?: 'MLFeatureProperties' } & Pick<Types.MlFeatureProperties, 'description' | 'dataType'> & {
                    version?: Types.Maybe<{ __typename?: 'VersionTag' } & Pick<Types.VersionTag, 'versionTag'>>;
                    sources?: Types.Maybe<
                        Array<
                            Types.Maybe<
                                { __typename?: 'Dataset' } & Pick<
                                    Types.Dataset,
                                    'urn' | 'name' | 'type' | 'origin' | 'description' | 'uri' | 'platformNativeType'
                                > & { platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment }
                            >
                        >
                    >;
                }
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        institutionalMemory?: Types.Maybe<{ __typename?: 'InstitutionalMemory' } & InstitutionalMemoryFieldsFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        tags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'MLFeatureEditableProperties' } & Pick<Types.MlFeatureEditableProperties, 'description'>
        >;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        featureTables?: Types.Maybe<
            { __typename?: 'EntityRelationshipsResult' } & {
                relationships: Array<
                    { __typename?: 'EntityRelationship' } & Pick<Types.EntityRelationship, 'type'> & {
                            entity?: Types.Maybe<
                                | { __typename?: 'AccessTokenMetadata' }
                                | { __typename?: 'Assertion' }
                                | { __typename?: 'Chart' }
                                | { __typename?: 'Container' }
                                | { __typename?: 'CorpGroup' }
                                | { __typename?: 'CorpUser' }
                                | { __typename?: 'Dashboard' }
                                | { __typename?: 'DataFlow' }
                                | { __typename?: 'DataHubPolicy' }
                                | { __typename?: 'DataHubRole' }
                                | { __typename?: 'DataHubView' }
                                | { __typename?: 'DataJob' }
                                | { __typename?: 'DataPlatform' }
                                | { __typename?: 'DataPlatformInstance' }
                                | { __typename?: 'DataProcessInstance' }
                                | { __typename?: 'Dataset' }
                                | { __typename?: 'Domain' }
                                | { __typename?: 'GlossaryNode' }
                                | { __typename?: 'GlossaryTerm' }
                                | { __typename?: 'MLFeature' }
                                | ({ __typename?: 'MLFeatureTable' } & {
                                      platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                                  })
                                | { __typename?: 'MLModel' }
                                | { __typename?: 'MLModelGroup' }
                                | { __typename?: 'MLPrimaryKey' }
                                | { __typename?: 'Notebook' }
                                | { __typename?: 'Post' }
                                | { __typename?: 'QueryEntity' }
                                | { __typename?: 'SchemaFieldEntity' }
                                | { __typename?: 'Tag' }
                                | { __typename?: 'Test' }
                                | { __typename?: 'VersionedDataset' }
                            >;
                        }
                >;
            }
        >;
    };

export type NonRecursiveMlPrimaryKeyFragment = { __typename?: 'MLPrimaryKey' } & Pick<
    Types.MlPrimaryKey,
    'urn' | 'type' | 'exists' | 'lastIngested' | 'name' | 'featureNamespace' | 'description' | 'dataType'
> & {
        properties?: Types.Maybe<
            { __typename?: 'MLPrimaryKeyProperties' } & Pick<
                Types.MlPrimaryKeyProperties,
                'description' | 'dataType'
            > & {
                    version?: Types.Maybe<{ __typename?: 'VersionTag' } & Pick<Types.VersionTag, 'versionTag'>>;
                    sources?: Types.Maybe<
                        Array<
                            Types.Maybe<
                                { __typename?: 'Dataset' } & Pick<
                                    Types.Dataset,
                                    'urn' | 'name' | 'type' | 'origin' | 'description' | 'uri' | 'platformNativeType'
                                > & { platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment }
                            >
                        >
                    >;
                }
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        institutionalMemory?: Types.Maybe<{ __typename?: 'InstitutionalMemory' } & InstitutionalMemoryFieldsFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        tags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'MLPrimaryKeyEditableProperties' } & Pick<
                Types.MlPrimaryKeyEditableProperties,
                'description'
            >
        >;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        featureTables?: Types.Maybe<
            { __typename?: 'EntityRelationshipsResult' } & {
                relationships: Array<
                    { __typename?: 'EntityRelationship' } & Pick<Types.EntityRelationship, 'type'> & {
                            entity?: Types.Maybe<
                                | { __typename?: 'AccessTokenMetadata' }
                                | { __typename?: 'Assertion' }
                                | { __typename?: 'Chart' }
                                | { __typename?: 'Container' }
                                | { __typename?: 'CorpGroup' }
                                | { __typename?: 'CorpUser' }
                                | { __typename?: 'Dashboard' }
                                | { __typename?: 'DataFlow' }
                                | { __typename?: 'DataHubPolicy' }
                                | { __typename?: 'DataHubRole' }
                                | { __typename?: 'DataHubView' }
                                | { __typename?: 'DataJob' }
                                | { __typename?: 'DataPlatform' }
                                | { __typename?: 'DataPlatformInstance' }
                                | { __typename?: 'DataProcessInstance' }
                                | { __typename?: 'Dataset' }
                                | { __typename?: 'Domain' }
                                | { __typename?: 'GlossaryNode' }
                                | { __typename?: 'GlossaryTerm' }
                                | { __typename?: 'MLFeature' }
                                | ({ __typename?: 'MLFeatureTable' } & {
                                      platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                                  })
                                | { __typename?: 'MLModel' }
                                | { __typename?: 'MLModelGroup' }
                                | { __typename?: 'MLPrimaryKey' }
                                | { __typename?: 'Notebook' }
                                | { __typename?: 'Post' }
                                | { __typename?: 'QueryEntity' }
                                | { __typename?: 'SchemaFieldEntity' }
                                | { __typename?: 'Tag' }
                                | { __typename?: 'Test' }
                                | { __typename?: 'VersionedDataset' }
                            >;
                        }
                >;
            }
        >;
    };

export type NonRecursiveMlFeatureTableFragment = { __typename?: 'MLFeatureTable' } & Pick<
    Types.MlFeatureTable,
    'urn' | 'type' | 'exists' | 'lastIngested' | 'name' | 'description'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        properties?: Types.Maybe<
            { __typename?: 'MLFeatureTableProperties' } & Pick<Types.MlFeatureTableProperties, 'description'> & {
                    mlFeatures?: Types.Maybe<
                        Array<Types.Maybe<{ __typename?: 'MLFeature' } & NonRecursiveMlFeatureFragment>>
                    >;
                    mlPrimaryKeys?: Types.Maybe<
                        Array<Types.Maybe<{ __typename?: 'MLPrimaryKey' } & NonRecursiveMlPrimaryKeyFragment>>
                    >;
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
        institutionalMemory?: Types.Maybe<{ __typename?: 'InstitutionalMemory' } & InstitutionalMemoryFieldsFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        tags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'MLFeatureTableEditableProperties' } & Pick<
                Types.MlFeatureTableEditableProperties,
                'description'
            >
        >;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
    };

export type SchemaFieldFieldsFragment = { __typename?: 'SchemaField' } & Pick<
    Types.SchemaField,
    | 'fieldPath'
    | 'label'
    | 'jsonPath'
    | 'nullable'
    | 'description'
    | 'type'
    | 'nativeDataType'
    | 'recursive'
    | 'isPartOfKey'
> & {
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
    };

export type SchemaMetadataFieldsFragment = { __typename?: 'SchemaMetadata' } & Pick<
    Types.SchemaMetadata,
    | 'aspectVersion'
    | 'createdAt'
    | 'datasetUrn'
    | 'name'
    | 'platformUrn'
    | 'version'
    | 'cluster'
    | 'hash'
    | 'primaryKeys'
> & {
        platformSchema?: Types.Maybe<
            | ({ __typename?: 'TableSchema' } & Pick<Types.TableSchema, 'schema'>)
            | ({ __typename?: 'KeyValueSchema' } & Pick<Types.KeyValueSchema, 'keySchema' | 'valueSchema'>)
        >;
        fields: Array<{ __typename?: 'SchemaField' } & SchemaFieldFieldsFragment>;
        foreignKeys?: Types.Maybe<
            Array<
                Types.Maybe<
                    { __typename?: 'ForeignKeyConstraint' } & Pick<Types.ForeignKeyConstraint, 'name'> & {
                            sourceFields?: Types.Maybe<
                                Array<
                                    Types.Maybe<
                                        { __typename?: 'SchemaFieldEntity' } & Pick<
                                            Types.SchemaFieldEntity,
                                            'fieldPath'
                                        >
                                    >
                                >
                            >;
                            foreignFields?: Types.Maybe<
                                Array<
                                    Types.Maybe<
                                        { __typename?: 'SchemaFieldEntity' } & Pick<
                                            Types.SchemaFieldEntity,
                                            'fieldPath'
                                        >
                                    >
                                >
                            >;
                            foreignDataset?: Types.Maybe<
                                { __typename?: 'Dataset' } & Pick<
                                    Types.Dataset,
                                    'urn' | 'name' | 'type' | 'origin' | 'uri' | 'platformNativeType'
                                > & {
                                        properties?: Types.Maybe<
                                            { __typename?: 'DatasetProperties' } & Pick<
                                                Types.DatasetProperties,
                                                'description'
                                            >
                                        >;
                                        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                                        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                                        globalTags?: Types.Maybe<
                                            { __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment
                                        >;
                                        glossaryTerms?: Types.Maybe<
                                            { __typename?: 'GlossaryTerms' } & GlossaryTermsFragment
                                        >;
                                    }
                            >;
                        }
                >
            >
        >;
    };

export type NonRecursiveMlModelFragment = { __typename?: 'MLModel' } & Pick<
    Types.MlModel,
    'urn' | 'type' | 'exists' | 'lastIngested' | 'name' | 'description' | 'origin'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        properties?: Types.Maybe<
            { __typename?: 'MLModelProperties' } & Pick<
                Types.MlModelProperties,
                'description' | 'date' | 'externalUrl' | 'version' | 'type' | 'mlFeatures'
            > & {
                    trainingMetrics?: Types.Maybe<
                        Array<
                            Types.Maybe<
                                { __typename?: 'MLMetric' } & Pick<Types.MlMetric, 'name' | 'description' | 'value'>
                            >
                        >
                    >;
                    hyperParams?: Types.Maybe<
                        Array<
                            Types.Maybe<
                                { __typename?: 'MLHyperParam' } & Pick<
                                    Types.MlHyperParam,
                                    'name' | 'description' | 'value'
                                >
                            >
                        >
                    >;
                    groups?: Types.Maybe<
                        Array<
                            Types.Maybe<
                                { __typename?: 'MLModelGroup' } & Pick<
                                    Types.MlModelGroup,
                                    'urn' | 'name' | 'description'
                                >
                            >
                        >
                    >;
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
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        tags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'MLModelEditableProperties' } & Pick<Types.MlModelEditableProperties, 'description'>
        >;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        institutionalMemory?: Types.Maybe<{ __typename?: 'InstitutionalMemory' } & InstitutionalMemoryFieldsFragment>;
    };

export type NonRecursiveMlModelGroupFieldsFragment = { __typename?: 'MLModelGroup' } & Pick<
    Types.MlModelGroup,
    'urn' | 'type' | 'exists' | 'lastIngested' | 'name' | 'description' | 'origin'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        tags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        editableProperties?: Types.Maybe<
            { __typename?: 'MLModelGroupEditableProperties' } & Pick<
                Types.MlModelGroupEditableProperties,
                'description'
            >
        >;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        properties?: Types.Maybe<
            { __typename?: 'MLModelGroupProperties' } & Pick<Types.MlModelGroupProperties, 'description'>
        >;
    };

export type PlatformFieldsFragment = { __typename?: 'DataPlatform' } & Pick<
    Types.DataPlatform,
    'urn' | 'type' | 'lastIngested' | 'name' | 'displayName'
> & {
        properties?: Types.Maybe<
            { __typename?: 'DataPlatformProperties' } & Pick<
                Types.DataPlatformProperties,
                'type' | 'displayName' | 'datasetNameDelimiter' | 'logoUrl'
            >
        >;
        info?: Types.Maybe<
            { __typename?: 'DataPlatformInfo' } & Pick<
                Types.DataPlatformInfo,
                'type' | 'displayName' | 'datasetNameDelimiter' | 'logoUrl'
            >
        >;
    };

export type NonConflictingPlatformFieldsFragment = { __typename?: 'DataPlatform' } & Pick<
    Types.DataPlatform,
    'urn' | 'type' | 'name' | 'displayName'
> & {
        properties?: Types.Maybe<
            { __typename?: 'DataPlatformProperties' } & Pick<
                Types.DataPlatformProperties,
                'displayName' | 'datasetNameDelimiter' | 'logoUrl'
            >
        >;
        info?: Types.Maybe<
            { __typename?: 'DataPlatformInfo' } & Pick<
                Types.DataPlatformInfo,
                'type' | 'displayName' | 'datasetNameDelimiter' | 'logoUrl'
            >
        >;
    };

export type DataPlatformInstanceFieldsFragment = { __typename?: 'DataPlatformInstance' } & Pick<
    Types.DataPlatformInstance,
    'urn' | 'type' | 'instanceId'
> & { platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment };

export type EntityContainerFragment = { __typename?: 'Container' } & Pick<Types.Container, 'urn'> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        properties?: Types.Maybe<{ __typename?: 'ContainerProperties' } & Pick<Types.ContainerProperties, 'name'>>;
        subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
    };

export type ParentContainerFieldsFragment = { __typename?: 'Container' } & Pick<Types.Container, 'urn'> & {
        properties?: Types.Maybe<{ __typename?: 'ContainerProperties' } & Pick<Types.ContainerProperties, 'name'>>;
    };

export type EntityDomainFragment = { __typename?: 'DomainAssociation' } & Pick<
    Types.DomainAssociation,
    'associatedUrn'
> & {
        domain: { __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'> & {
                properties?: Types.Maybe<
                    { __typename?: 'DomainProperties' } & Pick<Types.DomainProperties, 'name' | 'description'>
                >;
            };
    };

export type InputFieldsFieldsFragment = { __typename?: 'InputFields' } & {
    fields?: Types.Maybe<
        Array<
            Types.Maybe<
                { __typename?: 'InputField' } & Pick<Types.InputField, 'schemaFieldUrn'> & {
                        schemaField?: Types.Maybe<{ __typename?: 'SchemaField' } & SchemaFieldFieldsFragment>;
                    }
            >
        >
    >;
};

export const GlossaryNodeFragmentDoc = gql`
    fragment glossaryNode on GlossaryNode {
        urn
        type
        properties {
            name
        }
        children: relationships(input: { types: ["IsPartOf"], direction: INCOMING, start: 0, count: 10000 }) {
            total
        }
    }
`;
export const PlatformFieldsFragmentDoc = gql`
    fragment platformFields on DataPlatform {
        urn
        type
        lastIngested
        name
        properties {
            type
            displayName
            datasetNameDelimiter
            logoUrl
        }
        displayName
        info {
            type
            displayName
            datasetNameDelimiter
            logoUrl
        }
    }
`;
export const DataPlatformInstanceFieldsFragmentDoc = gql`
    fragment dataPlatformInstanceFields on DataPlatformInstance {
        urn
        type
        platform {
            ...platformFields
        }
        instanceId
    }
    ${PlatformFieldsFragmentDoc}
`;
export const OwnershipFieldsFragmentDoc = gql`
    fragment ownershipFields on Ownership {
        owners {
            owner {
                ... on CorpUser {
                    urn
                    type
                    username
                    info {
                        active
                        displayName
                        title
                        email
                        firstName
                        lastName
                        fullName
                    }
                    properties {
                        active
                        displayName
                        title
                        email
                        firstName
                        lastName
                        fullName
                    }
                    editableProperties {
                        displayName
                        title
                        pictureLink
                        email
                    }
                }
                ... on CorpGroup {
                    urn
                    type
                    name
                    properties {
                        displayName
                        email
                    }
                    info {
                        displayName
                        email
                        admins {
                            urn
                            username
                            info {
                                active
                                displayName
                                title
                                email
                                firstName
                                lastName
                                fullName
                            }
                            editableInfo {
                                pictureLink
                                teams
                                skills
                            }
                        }
                        members {
                            urn
                            username
                            info {
                                active
                                displayName
                                title
                                email
                                firstName
                                lastName
                                fullName
                            }
                            editableInfo {
                                pictureLink
                                teams
                                skills
                            }
                        }
                        groups
                    }
                }
            }
            type
            associatedUrn
        }
        lastModified {
            time
        }
    }
`;
export const InstitutionalMemoryFieldsFragmentDoc = gql`
    fragment institutionalMemoryFields on InstitutionalMemory {
        elements {
            url
            author {
                urn
                username
            }
            description
            created {
                actor
                time
            }
        }
    }
`;
export const GlobalTagsFieldsFragmentDoc = gql`
    fragment globalTagsFields on GlobalTags {
        tags {
            tag {
                urn
                type
                name
                description
                properties {
                    name
                    colorHex
                }
            }
            associatedUrn
        }
    }
`;
export const ParentNodesFieldsFragmentDoc = gql`
    fragment parentNodesFields on ParentNodesResult {
        count
        nodes {
            urn
            type
            properties {
                name
            }
        }
    }
`;
export const GlossaryTermFragmentDoc = gql`
    fragment glossaryTerm on GlossaryTerm {
        urn
        name
        type
        hierarchicalName
        properties {
            name
            description
            definition
            termSource
            customProperties {
                key
                value
            }
        }
        ownership {
            ...ownershipFields
        }
        parentNodes {
            ...parentNodesFields
        }
    }
    ${OwnershipFieldsFragmentDoc}
    ${ParentNodesFieldsFragmentDoc}
`;
export const GlossaryTermsFragmentDoc = gql`
    fragment glossaryTerms on GlossaryTerms {
        terms {
            term {
                ...glossaryTerm
            }
            associatedUrn
        }
    }
    ${GlossaryTermFragmentDoc}
`;
export const EntityDomainFragmentDoc = gql`
    fragment entityDomain on DomainAssociation {
        domain {
            urn
            type
            properties {
                name
                description
            }
        }
        associatedUrn
    }
`;
export const DeprecationFieldsFragmentDoc = gql`
    fragment deprecationFields on Deprecation {
        actor
        deprecated
        note
        decommissionTime
    }
`;
export const EntityContainerFragmentDoc = gql`
    fragment entityContainer on Container {
        urn
        platform {
            ...platformFields
        }
        properties {
            name
        }
        subTypes {
            typeNames
        }
        deprecation {
            ...deprecationFields
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
`;
export const EmbedFieldsFragmentDoc = gql`
    fragment embedFields on Embed {
        renderUrl
    }
`;
export const NonRecursiveDatasetFieldsFragmentDoc = gql`
    fragment nonRecursiveDatasetFields on Dataset {
        urn
        name
        type
        origin
        uri
        lastIngested
        platform {
            ...platformFields
        }
        dataPlatformInstance {
            ...dataPlatformInstanceFields
        }
        platformNativeType
        properties {
            name
            description
            customProperties {
                key
                value
            }
            externalUrl
        }
        editableProperties {
            description
        }
        ownership {
            ...ownershipFields
        }
        institutionalMemory {
            ...institutionalMemoryFields
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
        container {
            ...entityContainer
        }
        deprecation {
            ...deprecationFields
        }
        embed {
            ...embedFields
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${InstitutionalMemoryFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${EntityContainerFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${EmbedFieldsFragmentDoc}
`;
export const NonRecursiveDataJobFieldsFragmentDoc = gql`
    fragment nonRecursiveDataJobFields on DataJob {
        urn
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
        domain {
            ...entityDomain
        }
        deprecation {
            ...deprecationFields
        }
    }
    ${GlobalTagsFieldsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
`;
export const NonRecursiveDataFlowFieldsFragmentDoc = gql`
    fragment nonRecursiveDataFlowFields on DataFlow {
        urn
        type
        orchestrator
        flowId
        cluster
        properties {
            name
            description
            project
            externalUrl
            customProperties {
                key
                value
            }
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
        deprecation {
            ...deprecationFields
        }
    }
    ${OwnershipFieldsFragmentDoc}
    ${PlatformFieldsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
`;
export const DataJobFieldsFragmentDoc = gql`
    fragment dataJobFields on DataJob {
        urn
        type
        exists
        lastIngested
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
        editableProperties {
            description
        }
        globalTags {
            ...globalTagsFields
        }
        institutionalMemory {
            ...institutionalMemoryFields
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
        status {
            removed
        }
        dataPlatformInstance {
            ...dataPlatformInstanceFields
        }
        privileges {
            canEditLineage
        }
    }
    ${NonRecursiveDataFlowFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${InstitutionalMemoryFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
`;
export const ParentContainerFieldsFragmentDoc = gql`
    fragment parentContainerFields on Container {
        urn
        properties {
            name
        }
    }
`;
export const ParentContainersFieldsFragmentDoc = gql`
    fragment parentContainersFields on ParentContainersResult {
        count
        containers {
            ...parentContainerFields
        }
    }
    ${ParentContainerFieldsFragmentDoc}
`;
export const SchemaFieldFieldsFragmentDoc = gql`
    fragment schemaFieldFields on SchemaField {
        fieldPath
        label
        jsonPath
        nullable
        description
        type
        nativeDataType
        recursive
        isPartOfKey
        globalTags {
            ...globalTagsFields
        }
        glossaryTerms {
            ...glossaryTerms
        }
    }
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
`;
export const InputFieldsFieldsFragmentDoc = gql`
    fragment inputFieldsFields on InputFields {
        fields {
            schemaFieldUrn
            schemaField {
                ...schemaFieldFields
            }
        }
    }
    ${SchemaFieldFieldsFragmentDoc}
`;
export const DashboardFieldsFragmentDoc = gql`
    fragment dashboardFields on Dashboard {
        urn
        type
        exists
        lastIngested
        tool
        dashboardId
        properties {
            name
            description
            customProperties {
                key
                value
            }
            externalUrl
            access
            lastRefreshed
            created {
                time
            }
            lastModified {
                time
            }
        }
        editableProperties {
            description
        }
        ownership {
            ...ownershipFields
        }
        globalTags {
            ...globalTagsFields
        }
        institutionalMemory {
            ...institutionalMemoryFields
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
        embed {
            ...embedFields
        }
        deprecation {
            ...deprecationFields
        }
        dataPlatformInstance {
            ...dataPlatformInstanceFields
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
        inputFields {
            ...inputFieldsFields
        }
        subTypes {
            typeNames
        }
        privileges {
            canEditLineage
            canEditEmbed
        }
    }
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${InstitutionalMemoryFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${PlatformFieldsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${ParentContainersFieldsFragmentDoc}
    ${EmbedFieldsFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
    ${InputFieldsFieldsFragmentDoc}
`;
export const NonRecursiveMlFeatureFragmentDoc = gql`
    fragment nonRecursiveMLFeature on MLFeature {
        urn
        type
        exists
        lastIngested
        name
        featureNamespace
        description
        dataType
        properties {
            description
            dataType
            version {
                versionTag
            }
            sources {
                urn
                name
                type
                origin
                description
                uri
                platform {
                    ...platformFields
                }
                platformNativeType
            }
        }
        ownership {
            ...ownershipFields
        }
        institutionalMemory {
            ...institutionalMemoryFields
        }
        status {
            removed
        }
        glossaryTerms {
            ...glossaryTerms
        }
        domain {
            ...entityDomain
        }
        tags {
            ...globalTagsFields
        }
        editableProperties {
            description
        }
        deprecation {
            ...deprecationFields
        }
        dataPlatformInstance {
            ...dataPlatformInstanceFields
        }
        featureTables: relationships(input: { types: ["Contains"], direction: INCOMING, start: 0, count: 100 }) {
            relationships {
                type
                entity {
                    ... on MLFeatureTable {
                        platform {
                            ...platformFields
                        }
                    }
                }
            }
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${InstitutionalMemoryFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
`;
export const NonRecursiveMlPrimaryKeyFragmentDoc = gql`
    fragment nonRecursiveMLPrimaryKey on MLPrimaryKey {
        urn
        type
        exists
        lastIngested
        name
        featureNamespace
        description
        dataType
        properties {
            description
            dataType
            version {
                versionTag
            }
            sources {
                urn
                name
                type
                origin
                description
                uri
                platform {
                    ...platformFields
                }
                platformNativeType
            }
        }
        ownership {
            ...ownershipFields
        }
        institutionalMemory {
            ...institutionalMemoryFields
        }
        status {
            removed
        }
        glossaryTerms {
            ...glossaryTerms
        }
        domain {
            ...entityDomain
        }
        tags {
            ...globalTagsFields
        }
        editableProperties {
            description
        }
        deprecation {
            ...deprecationFields
        }
        dataPlatformInstance {
            ...dataPlatformInstanceFields
        }
        featureTables: relationships(input: { types: ["KeyedBy"], direction: INCOMING, start: 0, count: 100 }) {
            relationships {
                type
                entity {
                    ... on MLFeatureTable {
                        platform {
                            ...platformFields
                        }
                    }
                }
            }
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${InstitutionalMemoryFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
`;
export const NonRecursiveMlFeatureTableFragmentDoc = gql`
    fragment nonRecursiveMLFeatureTable on MLFeatureTable {
        urn
        type
        exists
        lastIngested
        name
        platform {
            ...platformFields
        }
        dataPlatformInstance {
            ...dataPlatformInstanceFields
        }
        description
        properties {
            description
            mlFeatures {
                ...nonRecursiveMLFeature
            }
            mlPrimaryKeys {
                ...nonRecursiveMLPrimaryKey
            }
            customProperties {
                key
                value
            }
        }
        ownership {
            ...ownershipFields
        }
        institutionalMemory {
            ...institutionalMemoryFields
        }
        status {
            removed
        }
        glossaryTerms {
            ...glossaryTerms
        }
        domain {
            ...entityDomain
        }
        tags {
            ...globalTagsFields
        }
        editableProperties {
            description
        }
        deprecation {
            ...deprecationFields
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
    ${NonRecursiveMlFeatureFragmentDoc}
    ${NonRecursiveMlPrimaryKeyFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${InstitutionalMemoryFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
`;
export const SchemaMetadataFieldsFragmentDoc = gql`
    fragment schemaMetadataFields on SchemaMetadata {
        aspectVersion
        createdAt
        datasetUrn
        name
        platformUrn
        version
        cluster
        hash
        platformSchema {
            ... on TableSchema {
                schema
            }
            ... on KeyValueSchema {
                keySchema
                valueSchema
            }
        }
        fields {
            ...schemaFieldFields
        }
        primaryKeys
        foreignKeys {
            name
            sourceFields {
                fieldPath
            }
            foreignFields {
                fieldPath
            }
            foreignDataset {
                urn
                name
                type
                origin
                uri
                properties {
                    description
                }
                platform {
                    ...platformFields
                }
                platformNativeType
                ownership {
                    ...ownershipFields
                }
                globalTags {
                    ...globalTagsFields
                }
                glossaryTerms {
                    ...glossaryTerms
                }
            }
        }
    }
    ${SchemaFieldFieldsFragmentDoc}
    ${PlatformFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
`;
export const NonRecursiveMlModelFragmentDoc = gql`
    fragment nonRecursiveMLModel on MLModel {
        urn
        type
        exists
        lastIngested
        name
        description
        origin
        platform {
            ...platformFields
        }
        dataPlatformInstance {
            ...dataPlatformInstanceFields
        }
        ownership {
            ...ownershipFields
        }
        properties {
            description
            date
            externalUrl
            version
            type
            trainingMetrics {
                name
                description
                value
            }
            hyperParams {
                name
                description
                value
            }
            mlFeatures
            groups {
                urn
                name
                description
            }
            customProperties {
                key
                value
            }
        }
        globalTags {
            ...globalTagsFields
        }
        status {
            removed
        }
        glossaryTerms {
            ...glossaryTerms
        }
        domain {
            ...entityDomain
        }
        tags {
            ...globalTagsFields
        }
        editableProperties {
            description
        }
        deprecation {
            ...deprecationFields
        }
        institutionalMemory {
            ...institutionalMemoryFields
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${InstitutionalMemoryFieldsFragmentDoc}
`;
export const NonRecursiveMlModelGroupFieldsFragmentDoc = gql`
    fragment nonRecursiveMLModelGroupFields on MLModelGroup {
        urn
        type
        exists
        lastIngested
        name
        description
        origin
        platform {
            ...platformFields
        }
        dataPlatformInstance {
            ...dataPlatformInstanceFields
        }
        ownership {
            ...ownershipFields
        }
        status {
            removed
        }
        glossaryTerms {
            ...glossaryTerms
        }
        domain {
            ...entityDomain
        }
        tags {
            ...globalTagsFields
        }
        editableProperties {
            description
        }
        deprecation {
            ...deprecationFields
        }
        properties {
            description
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
`;
export const NonConflictingPlatformFieldsFragmentDoc = gql`
    fragment nonConflictingPlatformFields on DataPlatform {
        urn
        type
        name
        properties {
            displayName
            datasetNameDelimiter
            logoUrl
        }
        displayName
        info {
            type
            displayName
            datasetNameDelimiter
            logoUrl
        }
    }
`;
