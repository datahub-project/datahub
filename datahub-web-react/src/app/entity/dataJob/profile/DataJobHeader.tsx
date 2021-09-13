import { Button, Divider, Row, Space, Typography } from 'antd';
import React from 'react';
import { FetchResult, MutationFunctionOptions } from '@apollo/client';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { DataJob, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import CompactContext from '../../../shared/CompactContext';
import { capitalizeFirstLetter } from '../../../shared/capitalizeFirstLetter';
import { AvatarsGroup } from '../../../shared/avatar';
import UpdatableDescription from '../../shared/components/legacy/UpdatableDescription';
import analytics, { EventType, EntityActionType } from '../../../analytics';

const PlatFormLink = styled(Typography.Text)`
    color: inherit;
`;

const ButtonContainer = styled.div`
    margin-top: 15px;
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
    const isCompact = React.useContext(CompactContext);
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
                        {!isCompact && info?.externalUrl && (
                            <Button onClick={openExternalUrl}>View in {platformName}</Button>
                        )}
                    </Space>
                    {isCompact && info?.externalUrl && (
                        <ButtonContainer>
                            <Button onClick={openExternalUrl}>View in {platformName}</Button>
                        </ButtonContainer>
                    )}
                </Row>
                <UpdatableDescription
                    isCompact={isCompact}
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
