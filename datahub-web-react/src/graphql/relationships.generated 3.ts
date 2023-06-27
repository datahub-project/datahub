/* eslint-disable */
import * as Types from '../types.generated';

import {
    LineageNodeProperties_Assertion_Fragment,
    LineageNodeProperties_Chart_Fragment,
    LineageNodeProperties_Dashboard_Fragment,
    LineageNodeProperties_DataFlow_Fragment,
    LineageNodeProperties_DataJob_Fragment,
    LineageNodeProperties_DataProcessInstance_Fragment,
    LineageNodeProperties_Dataset_Fragment,
    LineageNodeProperties_MlFeature_Fragment,
    LineageNodeProperties_MlFeatureTable_Fragment,
    LineageNodeProperties_MlModel_Fragment,
    LineageNodeProperties_MlModelGroup_Fragment,
    LineageNodeProperties_MlPrimaryKey_Fragment,
    LeafLineageResultsFragment,
} from './lineage.generated';
import { gql } from '@apollo/client';
import { LineageNodePropertiesFragmentDoc, LeafLineageResultsFragmentDoc } from './lineage.generated';
export type FullRelationshipResultsFragment = { __typename?: 'EntityRelationshipsResult' } & Pick<
    Types.EntityRelationshipsResult,
    'start' | 'count' | 'total'
> & {
        relationships: Array<
            { __typename?: 'EntityRelationship' } & Pick<Types.EntityRelationship, 'type' | 'direction'> & {
                    entity?: Types.Maybe<
                        | { __typename?: 'AccessTokenMetadata' }
                        | ({ __typename?: 'Assertion' } & RelationshipFields_Assertion_Fragment)
                        | ({ __typename?: 'Chart' } & RelationshipFields_Chart_Fragment)
                        | { __typename?: 'Container' }
                        | { __typename?: 'CorpGroup' }
                        | { __typename?: 'CorpUser' }
                        | ({ __typename?: 'Dashboard' } & RelationshipFields_Dashboard_Fragment)
                        | ({ __typename?: 'DataFlow' } & RelationshipFields_DataFlow_Fragment)
                        | { __typename?: 'DataHubPolicy' }
                        | { __typename?: 'DataHubRole' }
                        | { __typename?: 'DataHubView' }
                        | ({ __typename?: 'DataJob' } & RelationshipFields_DataJob_Fragment)
                        | { __typename?: 'DataPlatform' }
                        | { __typename?: 'DataPlatformInstance' }
                        | ({ __typename?: 'DataProcessInstance' } & RelationshipFields_DataProcessInstance_Fragment)
                        | ({ __typename?: 'Dataset' } & RelationshipFields_Dataset_Fragment)
                        | { __typename?: 'Domain' }
                        | { __typename?: 'GlossaryNode' }
                        | { __typename?: 'GlossaryTerm' }
                        | ({ __typename?: 'MLFeature' } & RelationshipFields_MlFeature_Fragment)
                        | ({ __typename?: 'MLFeatureTable' } & RelationshipFields_MlFeatureTable_Fragment)
                        | ({ __typename?: 'MLModel' } & RelationshipFields_MlModel_Fragment)
                        | ({ __typename?: 'MLModelGroup' } & RelationshipFields_MlModelGroup_Fragment)
                        | ({ __typename?: 'MLPrimaryKey' } & RelationshipFields_MlPrimaryKey_Fragment)
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
    };

export type LeafRelationshipResultsFragment = { __typename?: 'EntityRelationshipsResult' } & Pick<
    Types.EntityRelationshipsResult,
    'start' | 'count' | 'total'
> & {
        relationships: Array<
            { __typename?: 'EntityRelationship' } & Pick<Types.EntityRelationship, 'type'> & {
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
                }
        >;
    };

export type RelationshipFields_Assertion_Fragment = { __typename?: 'Assertion' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_Assertion_Fragment;

export type RelationshipFields_Chart_Fragment = { __typename?: 'Chart' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_Chart_Fragment;

export type RelationshipFields_Dashboard_Fragment = { __typename?: 'Dashboard' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_Dashboard_Fragment;

export type RelationshipFields_DataFlow_Fragment = { __typename?: 'DataFlow' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_DataFlow_Fragment;

export type RelationshipFields_DataJob_Fragment = { __typename?: 'DataJob' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_DataJob_Fragment;

export type RelationshipFields_DataProcessInstance_Fragment = { __typename?: 'DataProcessInstance' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_DataProcessInstance_Fragment;

export type RelationshipFields_Dataset_Fragment = { __typename?: 'Dataset' } & {
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
} & LineageNodeProperties_Dataset_Fragment;

export type RelationshipFields_MlFeature_Fragment = { __typename?: 'MLFeature' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_MlFeature_Fragment;

export type RelationshipFields_MlFeatureTable_Fragment = { __typename?: 'MLFeatureTable' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_MlFeatureTable_Fragment;

export type RelationshipFields_MlModel_Fragment = { __typename?: 'MLModel' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_MlModel_Fragment;

export type RelationshipFields_MlModelGroup_Fragment = { __typename?: 'MLModelGroup' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_MlModelGroup_Fragment;

export type RelationshipFields_MlPrimaryKey_Fragment = { __typename?: 'MLPrimaryKey' } & {
    upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
    downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & LeafLineageResultsFragment>;
} & LineageNodeProperties_MlPrimaryKey_Fragment;

export type RelationshipFieldsFragment =
    | RelationshipFields_Assertion_Fragment
    | RelationshipFields_Chart_Fragment
    | RelationshipFields_Dashboard_Fragment
    | RelationshipFields_DataFlow_Fragment
    | RelationshipFields_DataJob_Fragment
    | RelationshipFields_DataProcessInstance_Fragment
    | RelationshipFields_Dataset_Fragment
    | RelationshipFields_MlFeature_Fragment
    | RelationshipFields_MlFeatureTable_Fragment
    | RelationshipFields_MlModel_Fragment
    | RelationshipFields_MlModelGroup_Fragment
    | RelationshipFields_MlPrimaryKey_Fragment;

export const RelationshipFieldsFragmentDoc = gql`
    fragment relationshipFields on EntityWithRelationships {
        ...lineageNodeProperties
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
        upstream: lineage(input: { direction: UPSTREAM, start: 0, count: 100 }) {
            ...leafLineageResults
        }
        downstream: lineage(input: { direction: DOWNSTREAM, start: 0, count: 100 }) {
            ...leafLineageResults
        }
    }
    ${LineageNodePropertiesFragmentDoc}
    ${LeafLineageResultsFragmentDoc}
`;
export const FullRelationshipResultsFragmentDoc = gql`
    fragment fullRelationshipResults on EntityRelationshipsResult {
        start
        count
        total
        relationships {
            type
            direction
            entity {
                ...relationshipFields
            }
        }
    }
    ${RelationshipFieldsFragmentDoc}
`;
export const LeafRelationshipResultsFragmentDoc = gql`
    fragment leafRelationshipResults on EntityRelationshipsResult {
        start
        count
        total
        relationships {
            type
            entity {
                urn
                type
            }
        }
    }
`;
