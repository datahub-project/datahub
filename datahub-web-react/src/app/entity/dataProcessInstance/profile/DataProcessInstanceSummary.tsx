import { Pill } from '@components';
import { Space, Table, Typography } from 'antd';
import type { ColumnType } from 'antd/es/table';
import { capitalize } from 'lodash';
import React from 'react';
import { useTranslation } from 'react-i18next';
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

export default function DataProcessInstanceSummary() {
    const { t } = useTranslation('entity.types');
    const { t: tl } = useTranslation('common.labels');
    const baseEntity = useBaseEntity<GetDataProcessInstanceQuery>();
    const dpi = baseEntity?.dataProcessInstance;

    const metricsColumns: ColumnType<MlMetric>[] = [
        {
            title: tl('name'),
            dataIndex: 'name',
            width: 450,
            render: (name) => <span data-testid={`mlmodel-metric-row-${name}`}>{name}</span>,
        },
        {
            title: tl('value'),
            dataIndex: 'value',
        },
    ];

    const hyperparamsColumns: ColumnType<MlHyperParam>[] = [
        {
            title: tl('name'),
            dataIndex: 'name',
            width: 450,
            render: (name) => <span data-testid={`mlmodel-hyperparam-row-${name}`}>{name}</span>,
        },
        {
            title: tl('value'),
            dataIndex: 'value',
        },
    ];

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
                <Typography.Title level={3}>{tl('details')}</Typography.Title>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title={tl('createdAt')}>
                        <TimestampPopover timestamp={dpi?.properties?.created?.time} title={tl('createdAt')} />
                    </InfoItem>
                    <InfoItem title={tl('status')}>
                        <InfoItemContent>{formatStatus(dpi?.state)}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title={t('shared.durationColumn')}>
                        <InfoItemContent>{formatDuration(dpi?.state)}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title={t('shared.runIdColumn')}>
                        <InfoItemContent>{dpi?.mlTrainingRunProperties?.id}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title={t('shared.createdBy')}>
                        <InfoItemContent>{dpi?.properties?.created?.actor}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title={t('dataProcessInstance.artifactsLocation')}>
                        <InfoItemContent>{dpi?.mlTrainingRunProperties?.outputUrls}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
                <Typography.Title level={3}>{t('dataProcessInstance.trainingMetrics')}</Typography.Title>
                <div data-testid="mlmodel-training-metrics-table">
                    <Table
                        columns={metricsColumns}
                        dataSource={dpi?.mlTrainingRunProperties?.trainingMetrics as MlMetric[] | undefined}
                        pagination={false}
                    />
                </div>
                <Typography.Title level={3}>{t('dataProcessInstance.hyperParameters')}</Typography.Title>
                <div data-testid="mlmodel-hyperparams-table">
                    <Table
                        columns={hyperparamsColumns}
                        dataSource={dpi?.mlTrainingRunProperties?.hyperParams as MlHyperParam[] | undefined}
                        pagination={false}
                    />
                </div>
            </Space>
        </TabContent>
    );
}
