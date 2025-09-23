import { Pill } from '@components';
import { Space, Table, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { InfoItem } from '@app/entityV2/shared/components/styled/InfoItem';
import { notEmpty } from '@app/entityV2/shared/utils';
import { TimestampPopover } from '@app/sharedV2/TimestampPopover';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { colors } from '@src/alchemy-components/theme';

import { GetMlModelQuery } from '@graphql/mlModel.generated';
import { EntityType, MlHyperParam, MlMetric } from '@types';

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
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
`;

const JobLink = styled(Link)`
    color: ${colors.blue[700]};
    &:hover {
        text-decoration: underline;
    }
`;

export default function MLModelSummary() {
    const baseEntity = useBaseEntity<GetMlModelQuery>();
    const model = baseEntity?.mlModel;
    const entityRegistry = useEntityRegistry();

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

    const renderTrainingJobs = () => {
        const trainingJobs =
            model?.trainedBy?.relationships?.map((relationship) => relationship.entity).filter(notEmpty) || [];

        if (trainingJobs.length === 0) return '-';

        return (
            <div>
                {trainingJobs.map((job, index) => {
                    const { urn, name } = job as { urn: string; name?: string };
                    return (
                        <span key={urn}>
                            <JobLink to={entityRegistry.getEntityUrl(EntityType.DataProcessInstance, urn)}>
                                {name || urn}
                            </JobLink>
                            {index < trainingJobs.length - 1 && ', '}
                        </span>
                    );
                })}
            </div>
        );
    };

    return (
        <TabContent>
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                <Typography.Title level={3}>Model Details</Typography.Title>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title="Version">
                        <InfoItemContent>{model?.versionProperties?.version?.versionTag}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Registered At">
                        <TimestampPopover timestamp={model?.properties?.created?.time} title="Registered At" />
                    </InfoItem>
                    <InfoItem title="Last Modified At">
                        <TimestampPopover timestamp={model?.properties?.lastModified?.time} title="Last Modified At" />
                    </InfoItem>
                    <InfoItem title="Created By">
                        <InfoItemContent>{model?.properties?.created?.actor || '-'}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title="Aliases">
                        <InfoItemContent>
                            {model?.versionProperties?.aliases?.map((alias) => (
                                <Pill
                                    label={alias.versionTag ?? '-'}
                                    key={alias.versionTag}
                                    color="blue"
                                    clickable={false}
                                />
                            ))}
                        </InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Source Run">
                        <InfoItemContent>{renderTrainingJobs()}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
                <Typography.Title level={3}>Training Metrics</Typography.Title>
                <Table
                    pagination={false}
                    columns={propertyTableColumns}
                    dataSource={model?.properties?.trainingMetrics as MlMetric[]}
                />
                <Typography.Title level={3}>Hyper Parameters</Typography.Title>
                <Table
                    pagination={false}
                    columns={propertyTableColumns}
                    dataSource={model?.properties?.hyperParams as MlHyperParam[]}
                />
            </Space>
        </TabContent>
    );
}
