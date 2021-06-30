import React from 'react';
import { Alert } from 'antd';
import {
    useGetDatasetQuery,
    useUpdateDatasetMutation,
    GetDatasetDocument,
} from '../../../../graphql/dataset.generated';
import { Ownership as OwnershipView } from '../../shared/Ownership';
import SchemaView from './schema/Schema';
import { EntityProfile } from '../../../shared/EntityProfile';
import { Dataset, EntityType, GlobalTags, GlossaryTerms, SchemaMetadata } from '../../../../types.generated';
import LineageView from './Lineage';
import { Properties as PropertiesView } from '../../shared/Properties';
import DocumentsView from './Documentation';
import DatasetHeader from './DatasetHeader';
import { Message } from '../../../shared/Message';
import TagTermGroup from '../../../shared/tags/TagTermGroup';
import useIsLineageMode from '../../../lineage/utils/useIsLineageMode';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useGetAuthenticatedUser } from '../../../useGetAuthenticatedUser';
import analytics, { EventType, EntityActionType } from '../../../analytics';
import QueriesTab from './QueriesTab';

export enum TabType {
    Ownership = 'Ownership',
    Schema = 'Schema',
    Lineage = 'Lineage',
    Properties = 'Properties',
    Documents = 'Documents',
    Queries = 'Queries',
}

const EMPTY_ARR: never[] = [];

/**
 * Responsible for display the Dataset Page
 */
export const DatasetProfile = ({ urn }: { urn: string }): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    const { loading, error, data } = useGetDatasetQuery({ variables: { urn } });

    const user = useGetAuthenticatedUser();
    const [updateDataset] = useUpdateDatasetMutation({
        update(cache, { data: newDataset }) {
            cache.modify({
                fields: {
                    dataset() {
                        cache.writeQuery({
                            query: GetDatasetDocument,
                            data: { dataset: { ...newDataset?.updateDataset, usageStats: data?.dataset?.usageStats } },
                        });
                    },
                },
            });
        },
    });
    const isLineageMode = useIsLineageMode();

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || `Entity failed to load for urn ${urn}`} />;
    }

    const getHeader = (dataset: Dataset) => <DatasetHeader dataset={dataset} updateDataset={updateDataset} />;

    const getTabs = ({
        ownership,
        upstreamLineage,
        downstreamLineage,
        properties,
        institutionalMemory,
        schema,
        schemaMetadata,
        previousSchemaMetadata,
        editableSchemaMetadata,
        usageStats,
    }: Dataset & { previousSchemaMetadata: SchemaMetadata }) => {
        return [
            {
                name: TabType.Schema,
                path: TabType.Schema.toLowerCase(),
                content: (
                    <SchemaView
                        urn={urn}
                        schema={schemaMetadata || schema}
                        previousSchemaMetadata={previousSchemaMetadata}
                        usageStats={usageStats}
                        editableSchemaMetadata={editableSchemaMetadata}
                        updateEditableSchema={(update) => {
                            analytics.event({
                                type: EventType.EntityActionEvent,
                                actionType: EntityActionType.UpdateSchemaDescription,
                                entityType: EntityType.Dataset,
                                entityUrn: urn,
                            });
                            return updateDataset({ variables: { input: { urn, editableSchemaMetadata: update } } });
                        }}
                    />
                ),
            },
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLowerCase(),
                content: (
                    <OwnershipView
                        owners={(ownership && ownership.owners) || EMPTY_ARR}
                        lastModifiedAt={(ownership && ownership.lastModified?.time) || 0}
                        updateOwnership={(update) => {
                            analytics.event({
                                type: EventType.EntityActionEvent,
                                actionType: EntityActionType.UpdateOwnership,
                                entityType: EntityType.Dataset,
                                entityUrn: urn,
                            });
                            return updateDataset({ variables: { input: { urn, ownership: update } } });
                        }}
                    />
                ),
            },
            {
                name: TabType.Lineage,
                path: TabType.Lineage.toLowerCase(),
                content: <LineageView upstreamLineage={upstreamLineage} downstreamLineage={downstreamLineage} />,
            },
            {
                name: TabType.Queries,
                path: TabType.Queries.toLowerCase(),
                content: <QueriesTab usageStats={usageStats} />,
            },
            {
                name: TabType.Properties,
                path: TabType.Properties.toLowerCase(),
                content: <PropertiesView properties={properties || EMPTY_ARR} />,
            },
            {
                name: TabType.Documents,
                path: 'docs',
                content: (
                    <DocumentsView
                        authenticatedUserUrn={user?.urn}
                        authenticatedUserUsername={user?.username}
                        documents={institutionalMemory?.elements || EMPTY_ARR}
                        updateDocumentation={(update) => {
                            analytics.event({
                                type: EventType.EntityActionEvent,
                                actionType: EntityActionType.UpdateDocumentation,
                                entityType: EntityType.Dataset,
                                entityUrn: urn,
                            });
                            return updateDataset({ variables: { input: { urn, institutionalMemory: update } } });
                        }}
                    />
                ),
            },
        ];
    };

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.dataset && (
                <EntityProfile
                    titleLink={`/${entityRegistry.getPathName(
                        EntityType.Dataset,
                    )}/${urn}?is_lineage_mode=${isLineageMode}`}
                    title={data.dataset.name}
                    tags={
                        <TagTermGroup
                            editableTags={data.dataset?.globalTags as GlobalTags}
                            glossaryTerms={data.dataset?.glossaryTerms as GlossaryTerms}
                            canAdd
                            canRemove
                            updateTags={(globalTags) => {
                                analytics.event({
                                    type: EventType.EntityActionEvent,
                                    actionType: EntityActionType.UpdateTags,
                                    entityType: EntityType.Dataset,
                                    entityUrn: urn,
                                });
                                return updateDataset({ variables: { input: { urn, globalTags } } });
                            }}
                        />
                    }
                    tagCardHeader={data.dataset?.glossaryTerms ? 'Tags & Terms' : 'Tags'}
                    tabs={getTabs(data.dataset as Dataset & { previousSchemaMetadata: SchemaMetadata })}
                    header={getHeader(data.dataset as Dataset)}
                    onTabChange={(tab: string) => {
                        analytics.event({
                            type: EventType.EntitySectionViewEvent,
                            entityType: EntityType.Dataset,
                            entityUrn: urn,
                            section: tab,
                        });
                    }}
                />
            )}
        </>
    );
};
