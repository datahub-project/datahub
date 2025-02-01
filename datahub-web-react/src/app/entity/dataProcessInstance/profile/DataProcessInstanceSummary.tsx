import React from 'react';
import styled from 'styled-components';
import { Space, Table, Typography } from 'antd';
import { formatDetailedDuration } from '@src/app/shared/time/timeUtils';
import { capitalize } from 'lodash';
import moment from 'moment';
import { MlHyperParam, MlMetric, DataProcessInstanceRunResultType } from '../../../../types.generated';
import { useBaseEntity } from '../../shared/EntityContext';
import { InfoItem } from '../../shared/components/styled/InfoItem';
import { GetDataProcessInstanceQuery } from '../../../../graphql/dataProcessInstance.generated';
import { Pill } from '../../../../alchemy-components/components/Pills';

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

export default function MLModelSummary() {
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
                        <InfoItemContent>
                            {dpi?.properties?.created?.time
                                ? moment(dpi.properties.created.time).format('YYYY-MM-DD HH:mm:ss')
                                : '-'}
                        </InfoItemContent>
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
