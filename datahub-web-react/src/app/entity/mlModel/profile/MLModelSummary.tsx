import React from 'react';
import styled from 'styled-components';
import { Space, Table, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { colors } from '@src/alchemy-components/theme';
import moment from 'moment';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { MlHyperParam, MlMetric, EntityType } from '../../../../types.generated';
import { useBaseEntity } from '../../shared/EntityContext';
import { GetMlModelQuery } from '../../../../graphql/mlModel.generated';
import { InfoItem } from '../../shared/components/styled/InfoItem';
import { notEmpty } from '../../shared/utils';
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
                        <InfoItemContent>
                            {model?.properties?.created?.time
                                ? moment(model.properties.created.time).format('YYYY-MM-DD HH:mm:ss')
                                : '-'}
                        </InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Last Modified At">
                        <InfoItemContent>
                            {model?.properties?.lastModified?.time
                                ? moment(model.properties.lastModified.time).format('YYYY-MM-DD HH:mm:ss')
                                : '-'}
                        </InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Created By">
                        <InfoItemContent>{model?.properties?.created?.actor}</InfoItemContent>
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
