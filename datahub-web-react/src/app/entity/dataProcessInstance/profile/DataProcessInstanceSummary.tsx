import { Pill } from '@components';
import { Space, Table, Typography } from 'antd';
import { capitalize } from 'lodash';
import React from 'react';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { InfoItem } from '@app/entity/shared/components/styled/InfoItem';
import { TimestampPopover } from '@app/sharedV2/TimestampPopover';
import { formatDetailedDuration } from '@src/app/shared/time/timeUtils';

import { GetDataProcessInstanceQuery } from '@graphql/dataProcessInstance.generated';
import { DataProcessInstanceRunResultType, MlHyperParam, MlMetric } from '@types';

const TabContent = styled.div`
    padding: 16px;
`;

const InfoItemContainer = styled.div<{ justifyContent }>`
    display: flex;
    position: relative;
    justify-content: ${(props) => props.justifyContent};
    padding: 0px 2px;
`;

const InfoItemContent = styled.div`
    padding-top: 8px;
    width: 100px;
    overflow-wrap: break-word;
`;

const propertyTableColumns = [
    {
        title: 'Name',
        dataIndex: 'name',
        width: 450,
    },
    {
        title: 'Value',
        dataIndex: 'value',
    },
];

export default function DataProcessInstanceSummary() {
    const baseEntity = useBaseEntity<GetDataProcessInstanceQuery>();
    const dpi = baseEntity?.dataProcessInstance;

    const formatStatus = (state) => {
        if (!state || state.length === 0) return '-';
        const result = state[0]?.result?.resultType;
        const statusColor = result === DataProcessInstanceRunResultType.Success ? 'green' : 'red';
        return <Pill label={capitalize(result)} color={statusColor} clickable={false} />;
    };

    const formatDuration = (state) => {
        if (!state || state.length === 0) return '-';
        return formatDetailedDuration(state[0]?.durationMillis);
    };

    return (
        <TabContent>
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                <Typography.Title level={3}>Details</Typography.Title>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title="Created At">
                        <TimestampPopover timestamp={dpi?.properties?.created?.time} title="Created At" />
                    </InfoItem>
                    <InfoItem title="Status">
                        <InfoItemContent>{formatStatus(dpi?.state)}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Duration">
                        <InfoItemContent>{formatDuration(dpi?.state)}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Run ID">
                        <InfoItemContent>{dpi?.mlTrainingRunProperties?.id}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Created By">
                        <InfoItemContent>{dpi?.properties?.created?.actor}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title="Artifacts Location">
                        <InfoItemContent>{dpi?.mlTrainingRunProperties?.outputUrls}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
                <Typography.Title level={3}>Training Metrics</Typography.Title>
                <Table
                    pagination={false}
                    columns={propertyTableColumns}
                    dataSource={dpi?.mlTrainingRunProperties?.trainingMetrics as MlMetric[]}
                />
                <Typography.Title level={3}>Hyper Parameters</Typography.Title>
                <Table
                    pagination={false}
                    columns={propertyTableColumns}
                    dataSource={dpi?.mlTrainingRunProperties?.hyperParams as MlHyperParam[]}
                />
            </Space>
        </TabContent>
    );
}
