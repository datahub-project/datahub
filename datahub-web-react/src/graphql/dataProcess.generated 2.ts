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
export type RunResultsFragment = { __typename?: 'DataProcessInstanceResult' } & Pick<
    Types.DataProcessInstanceResult,
    'count' | 'start' | 'total'
> & {
        runs?: Types.Maybe<
            Array<
                Types.Maybe<
                    { __typename?: 'DataProcessInstance' } & Pick<
                        Types.DataProcessInstance,
                        'urn' | 'type' | 'name' | 'externalUrl'
                    > & {
                            created?: Types.Maybe<
                                { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time' | 'actor'>
                            >;
                            state?: Types.Maybe<
                                Array<
                                    Types.Maybe<
                                        { __typename?: 'DataProcessRunEvent' } & Pick<
                                            Types.DataProcessRunEvent,
                                            'status' | 'attempt' | 'timestampMillis'
                                        > & {
                                                result?: Types.Maybe<
                                                    { __typename?: 'DataProcessInstanceRunResult' } & Pick<
                                                        Types.DataProcessInstanceRunResult,
                                                        'resultType' | 'nativeResultType'
                                                    >
                                                >;
                                            }
                                    >
                                >
                            >;
                            inputs?: Types.Maybe<
                                { __typename?: 'EntityRelationshipsResult' } & RunRelationshipResultsFragment
                            >;
                            outputs?: Types.Maybe<
                                { __typename?: 'EntityRelationshipsResult' } & RunRelationshipResultsFragment
                            >;
                            parentTemplate?: Types.Maybe<
                                { __typename?: 'EntityRelationshipsResult' } & RunRelationshipResultsFragment
                            >;
                        }
                >
            >
        >;
    };

export type RunRelationshipResultsFragment = { __typename?: 'EntityRelationshipsResult' } & Pick<
    Types.EntityRelationshipsResult,
    'start' | 'count' | 'total'
> & {
        relationships: Array<
            { __typename?: 'EntityRelationship' } & Pick<Types.EntityRelationship, 'type' | 'direction'> & {
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
                        | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type' | 'jobId'> & {
                                  dataFlow?: Types.Maybe<
                                      { __typename?: 'DataFlow' } & NonRecursiveDataFlowFieldsFragment
                                  >;
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
                                  deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
                                  dataPlatformInstance?: Types.Maybe<
                                      { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
                                  >;
                                  editableProperties?: Types.Maybe<
                                      { __typename?: 'DataJobEditableProperties' } & Pick<
                                          Types.DataJobEditableProperties,
                                          'description'
                                      >
                                  >;
                                  status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
                              })
                        | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
                        | ({ __typename?: 'DataPlatformInstance' } & Pick<Types.DataPlatformInstance, 'urn' | 'type'>)
                        | ({ __typename?: 'DataProcessInstance' } & Pick<Types.DataProcessInstance, 'urn' | 'type'>)
                        | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'name' | 'urn' | 'type'> & {
                                  properties?: Types.Maybe<
                                      { __typename?: 'DatasetProperties' } & Pick<
                                          Types.DatasetProperties,
                                          'name' | 'description' | 'qualifiedName'
                                      >
                                  >;
                                  editableProperties?: Types.Maybe<
                                      { __typename?: 'DatasetEditableProperties' } & Pick<
                                          Types.DatasetEditableProperties,
                                          'description'
                                      >
                                  >;
                                  platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                                  subTypes?: Types.Maybe<
                                      { __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>
                                  >;
                                  status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
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
                    >;
                }
        >;
    };

export const RunRelationshipResultsFragmentDoc = gql`
    fragment runRelationshipResults on EntityRelationshipsResult {
        start
        count
        total
        relationships {
            type
            direction
            entity {
                urn
                type
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
                    subTypes {
                        typeNames
                    }
                    status {
                        removed
                    }
                }
                ... on DataJob {
                    urn
                    type
                    dataFlow {
                        ...nonRecursiveDataFlowFields
                    }
                    jobId
                    properties {
                        name
                        description
                        externalUrl
                        customProperties {
                            key
                            value
                        }
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
            }
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${NonRecursiveDataFlowFieldsFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
`;
export const RunResultsFragmentDoc = gql`
    fragment runResults on DataProcessInstanceResult {
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
            name
            state(startTimeMillis: null, endTimeMillis: null, limit: 1) {
                status
                attempt
                result {
                    resultType
                    nativeResultType
                }
                timestampMillis
            }
            inputs: relationships(input: { types: ["Consumes"], direction: OUTGOING, start: 0, count: 20 }) {
                ...runRelationshipResults
            }
            outputs: relationships(input: { types: ["Produces"], direction: OUTGOING, start: 0, count: 20 }) {
                ...runRelationshipResults
            }
            parentTemplate: relationships(input: { types: ["InstanceOf"], direction: OUTGOING, start: 0, count: 1 }) {
                ...runRelationshipResults
            }
            externalUrl
        }
    }
    ${RunRelationshipResultsFragmentDoc}
`;
