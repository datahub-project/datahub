import React from 'react';
import { Avatar, Tooltip, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { useGetDatasetQuery } from '../../../../graphql/dataset.generated';
import defaultAvatar from '../../../../images/default_avatar.png';
import { Ownership as OwnershipView } from './Ownership';
import { Schema as SchemaView } from './Schema';
import { EntityProfile } from '../../../shared/EntityProfile';
import { Dataset, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import LineageView from './Lineage';
import { sampleDownstreamEntities, sampleUpstreamEntities } from './stories/lineageEntities';

export enum TabType {
    Ownership = 'Ownership',
    Schema = 'Schema',
    Lineage = 'Lineage',
}

const ENABLED_TAB_TYPES = [TabType.Ownership, TabType.Schema, TabType.Lineage];
const EMPTY_OWNER_ARR: never[] = [];

/**
 * Responsible for display the Dataset Page
 */
export const Profile = ({ urn }: { urn: string }): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    const { loading, error, data } = useGetDatasetQuery({ variables: { urn } });

    const getBody = (description: string, ownership: any) => (
        <>
            <Typography.Paragraph>{description}</Typography.Paragraph>
            <Avatar.Group maxCount={6} size="large">
                {ownership &&
                    ownership.owners &&
                    ownership.owners.map((owner: any) => (
                        <Tooltip title={owner.owner.info?.fullName}>
                            <Link to={`/${entityRegistry.getPathName(EntityType.User)}/${owner.owner.urn}`}>
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

    const getTabs = ({ ownership }: Dataset) => {
        return [
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLowerCase(),
                content: (
                    <OwnershipView
                        initialOwners={(ownership && ownership.owners) || EMPTY_OWNER_ARR}
                        lastModifiedAt={(ownership && ownership.lastModified) || 0}
                    />
                ),
            },
            {
                name: TabType.Schema,
                path: TabType.Schema.toLowerCase(),
                content: <SchemaView />,
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
                    tabs={getTabs(data.dataset)}
                />
            )}
            {error && <p>Failed to load dataset with urn {urn}</p>}
        </>
    );
};
