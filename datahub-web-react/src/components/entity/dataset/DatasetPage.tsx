import * as React from 'react';
import { Link, useParams } from 'react-router-dom';
import { Avatar, Col, Row, Tooltip } from 'antd';

import { BrowsableEntityPage } from '../../browse/BrowsableEntityPage';
import { useGetDatasetQuery } from '../../../graphql/dataset.generated';
import { EntityType } from '../../shared/EntityTypeUtil';
import defaultAvatar from '../../../images/default_avatar.png';
import { Ownership as OwnershipView } from './Ownership';
import { Schema as SchemaView } from './Schema';
import { GenericEntityDetails } from '../../shared/GenericEntityDetails';
import { PageRoutes } from '../../../conf/Global';

export enum TabType {
    Ownership = 'Ownership',
    Schema = 'Schema',
}

const ENABLED_TAB_TYPES = [TabType.Ownership, TabType.Schema];

interface RouteParams {
    urn: string;
}

/**
 * Responsible for display the Dataset Page
 */
export const DatasetPage: React.VFC = () => {
    const { urn } = useParams<RouteParams>();

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
                                    <Link to={`${PageRoutes.USERS}/${owner.owner.urn}`}>
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
                        owners={(ownership && ownership.owners) || []}
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
        <BrowsableEntityPage urn={urn} type={EntityType.Dataset}>
            {loading && <p>Loading...</p>}
            {data && !data.dataset && !error && <p>Unable to find dataset with urn {urn}</p>}
            {data && data.dataset && !error && (
                <GenericEntityDetails
                    title={data.dataset.name}
                    tags={data.dataset.tags}
                    body={getBody(data.dataset?.description || '', data.dataset?.ownership)}
                    tabs={getTabs(data.dataset)}
                />
            )}
            {error && <p>Failed to load dataset with urn {urn}</p>}
        </BrowsableEntityPage>
    );
};
