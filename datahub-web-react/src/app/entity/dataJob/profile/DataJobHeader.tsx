import { Button, Divider, Row, Space, Typography } from 'antd';
import React from 'react';
import { FetchResult, MutationFunctionOptions } from '@apollo/client';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { DataJob, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import { AvatarsGroup } from '../../../shared/avatar';
import UpdatableDescription from '../../shared/UpdatableDescription';
import analytics, { EventType, EntityActionType } from '../../../analytics';

const PlatFormLink = styled(Typography.Text)`
    color: inherit;
`;

export type Props = {
    dataJob: DataJob;
    updateDataJob: (options?: MutationFunctionOptions<any, any> | undefined) => Promise<FetchResult>;
};

export default function DataJobHeader({
    dataJob: { urn, type, ownership, info, dataFlow, editableProperties },
    updateDataJob,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const platformName = dataFlow?.orchestrator ? capitalizeFirstLetter(dataFlow.orchestrator) : '';

    const openExternalUrl = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.ClickExternalUrl,
            entityType: EntityType.DataJob,
            entityUrn: urn,
        });
        window.open(info?.externalUrl || undefined, '_blank');
    };

    return (
        <>
            <Space direction="vertical" size="middle">
                <Row justify="space-between">
                    <Space split={<Divider type="vertical" />}>
                        <Typography.Text>Data Task</Typography.Text>
                        {dataFlow && dataFlow.urn && (
                            <Link to={`/${entityRegistry.getPathName(EntityType.DataFlow)}/${dataFlow.urn}`}>
                                <PlatFormLink strong>{`Pipeline: ${dataFlow.flowId}`}</PlatFormLink>
                            </Link>
                        )}
                        {dataFlow && dataFlow?.orchestrator && (
                            <Link
                                to={`/browse/${entityRegistry.getPathName(EntityType.DataFlow)}/${
                                    dataFlow?.orchestrator
                                }`}
                            >
                                <PlatFormLink strong>{platformName}</PlatFormLink>
                            </Link>
                        )}
                        {info?.externalUrl && <Button onClick={openExternalUrl}>View in {platformName}</Button>}
                    </Space>
                </Row>
                <UpdatableDescription
                    updateEntity={updateDataJob}
                    updatedDescription={editableProperties?.description}
                    originalDescription={info?.description}
                    entityType={type}
                    urn={urn}
                />
                <AvatarsGroup owners={ownership?.owners} entityRegistry={entityRegistry} size="large" />
            </Space>
        </>
    );
}
