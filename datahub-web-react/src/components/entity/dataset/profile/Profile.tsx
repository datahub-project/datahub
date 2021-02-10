import React from 'react';
import { Avatar, Tooltip, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { useGetDatasetQuery, useUpdateDatasetMutation } from '../../../../graphql/dataset.generated';
import defaultAvatar from '../../../../images/default_avatar.png';
import { Ownership as OwnershipView } from './Ownership';
import SchemaView from './schema/Schema';
import { EntityProfile } from '../../../shared/EntityProfile';
import { Dataset, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import LineageView from './Lineage';
import PropertiesView from './Properties';
import DocumentsView from './Documentation';
import { sampleDownstreamEntities, sampleUpstreamEntities } from './stories/lineageEntities';

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
export const Profile = ({ urn }: { urn: string }): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    const { loading, error, data } = useGetDatasetQuery({ variables: { urn } });

    const [updateDataset] = useUpdateDatasetMutation();

    const getBody = (description: string, ownership: any) => (
        <>
            <Typography.Paragraph>{description}</Typography.Paragraph>
            <Avatar.Group maxCount={6} size="large">
                {ownership &&
                    ownership.owners &&
                    ownership.owners.map((owner: any) => (
                        <Tooltip title={owner.owner.info?.fullName}>
                            <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${owner.owner.urn}`}>
                                <Avatar
                                    style={{
                                        color: '#f56a00',
                                        backgroundColor: '#fde3cf',
                                    }}
                                    src={
                                        (owner.owner.editableInfo && owner.owner.editableInfo.pictureLink) ||
                                        defaultAvatar
                                    }
                                />
                            </Link>
                        </Tooltip>
                    ))}
            </Avatar.Group>
        </>
    );

    const getTabs = ({ ownership, properties, institutionalMemory, schema }: Dataset) => {
        return [
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
                name: TabType.Schema,
                path: TabType.Schema.toLowerCase(),
                content: <SchemaView schema={schema} />,
            },
            {
                name: TabType.Lineage,
                path: TabType.Lineage.toLowerCase(),
                content: (
                    <LineageView
                        upstreamEntities={sampleUpstreamEntities}
                        downstreamEntities={sampleDownstreamEntities}
                    />
                ),
            },
            {
                name: TabType.Properties,
                path: TabType.Properties.toLowerCase(),
                content: <PropertiesView properties={properties || EMPTY_ARR} />,
            },
            {
                name: TabType.Documents,
                path: 'Docs',
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
            {loading && <p>Loading...</p>}
            {data && !data.dataset && !error && <p>Unable to find dataset with urn {urn}</p>}
            {data && data.dataset && !error && (
                <EntityProfile
                    title={data.dataset.name}
                    tags={data.dataset.tags}
                    body={getBody(data.dataset?.description || '', data.dataset?.ownership)}
                    tabs={getTabs(data.dataset as Dataset)}
                />
            )}
            {error && <p>Failed to load dataset with urn {urn}</p>}
        </>
    );
};
