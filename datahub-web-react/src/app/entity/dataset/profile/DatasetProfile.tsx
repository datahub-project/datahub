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
import { Dataset, GlobalTags } from '../../../../types.generated';
import LineageView from './Lineage';
import PropertiesView from './Properties';
import DocumentsView from './Documentation';
import DatasetHeader from './DatasetHeader';
import { Message } from '../../../shared/Message';
import TagGroup from '../../../shared/tags/TagGroup';
import useIsLineageMode from '../../../lineage/utils/useIsLineageMode';

export enum TabType {
    Ownership = 'Ownership',
    Schema = 'Schema',
    Lineage = 'Lineage',
    Properties = 'Properties',
    Documents = 'Documents',
}

const ENABLED_TAB_TYPES = [TabType.Ownership, TabType.Schema, TabType.Lineage, TabType.Properties, TabType.Documents];
const EMPTY_ARR: never[] = [];

/**
 * Responsible for display the Dataset Page
 */
export const DatasetProfile = ({ urn }: { urn: string }): JSX.Element => {
    const { loading, error, data } = useGetDatasetQuery({ variables: { urn } });
    const [updateDataset] = useUpdateDatasetMutation({
        update(cache, { data: newDataset }) {
            cache.modify({
                fields: {
                    dataset() {
                        cache.writeQuery({
                            query: GetDatasetDocument,
                            data: { dataset: newDataset?.updateDataset },
                        });
                    },
                },
            });
        },
    });
    const isLineageMode = useIsLineageMode();

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const getHeader = (dataset: Dataset) => <DatasetHeader dataset={dataset} />;

    const getTabs = ({
        ownership,
        upstreamLineage,
        downstreamLineage,
        properties,
        institutionalMemory,
        schema,
        editableSchemaMetadata,
    }: Dataset) => {
        return [
            {
                name: TabType.Schema,
                path: TabType.Schema.toLowerCase(),
                content: (
                    <SchemaView
                        schema={schema}
                        editableSchemaMetadata={editableSchemaMetadata}
                        updateEditableSchema={(update) =>
                            updateDataset({ variables: { input: { urn, editableSchemaMetadata: update } } })
                        }
                    />
                ),
            },
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLowerCase(),
                content: (
                    <OwnershipView
                        owners={(ownership && ownership.owners) || EMPTY_ARR}
                        lastModifiedAt={(ownership && ownership.lastModified.time) || 0}
                        updateOwnership={(update) =>
                            updateDataset({ variables: { input: { urn, ownership: update } } })
                        }
                    />
                ),
            },
            {
                name: TabType.Lineage,
                path: TabType.Lineage.toLowerCase(),
                content: <LineageView upstreamLineage={upstreamLineage} downstreamLineage={downstreamLineage} />,
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
                        documents={institutionalMemory?.elements || EMPTY_ARR}
                        updateDocumentation={(update) =>
                            updateDataset({ variables: { input: { urn, institutionalMemory: update } } })
                        }
                    />
                ),
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    return (
        <>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {data && data.dataset && (
                <EntityProfile
                    titleLink={data.dataset.urn && `/dataset/${data.dataset.urn}?is_lineage_mode=${isLineageMode}`}
                    title={data.dataset.name}
                    tags={
                        <TagGroup
                            editableTags={data.dataset?.globalTags as GlobalTags}
                            canAdd
                            canRemove
                            updateTags={(globalTags) => updateDataset({ variables: { input: { urn, globalTags } } })}
                        />
                    }
                    tabs={getTabs(data.dataset as Dataset)}
                    header={getHeader(data.dataset as Dataset)}
                />
            )}
        </>
    );
};
