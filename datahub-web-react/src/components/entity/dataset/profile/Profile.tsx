import React from 'react';
import { Avatar, Col, Row, Tooltip } from 'antd';
import { Link } from 'react-router-dom';
import { useGetDatasetQuery } from '../../../../graphql/dataset.generated';
import defaultAvatar from '../../../../images/default_avatar.png';
import { Ownership as OwnershipView } from './Ownership';
import { Schema as SchemaView } from './Schema';
import { EntityProfile } from '../../../shared/EntityProfile';
import { EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';

export enum TabType {
    Ownership = 'Ownership',
    Schema = 'Schema',
}

const ENABLED_TAB_TYPES = [TabType.Ownership, TabType.Schema];
const EMPTY_OWNER_ARR: never[] = [];

/**
 * Responsible for display the Dataset Page
 */
export const Profile = ({ urn }: { urn: string }): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    const { loading, error, data } = useGetDatasetQuery({ variables: { urn } });

    const getBody = (description: string, ownership: any) => (
        <>
            <Row>
                <Col span={10}>
                    <p>{description}</p>
                </Col>
            </Row>
            <Row style={{ padding: '20px 0px' }}>
                <Col span={24}>
                    <Avatar.Group maxCount={6} size="large">
                        {ownership &&
                            ownership.owners &&
                            ownership.owners.map((owner: any) => (
                                <Tooltip title={owner.owner.info?.fullName}>
                                    <Link to={`${entityRegistry.getPathName(EntityType.User)}/${owner.owner.urn}`}>
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
                </Col>
            </Row>
        </>
    );

    const getTabs = ({ ownership }: { urn: string; ownership?: any }) => {
        return [
            {
                name: TabType.Ownership,
                path: TabType.Ownership.toLowerCase(),
                content: (
                    <OwnershipView
                        initialOwners={(ownership && ownership.owners) || EMPTY_OWNER_ARR}
                        lastModifiedAt={ownership && ownership.lastModified}
                    />
                ),
            },
            {
                name: TabType.Schema,
                path: TabType.Schema.toLowerCase(),
                content: <SchemaView />,
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
