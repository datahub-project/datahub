import React from 'react';
import styled from 'styled-components';
import { Space, Table, Typography } from 'antd';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { MlHyperParam, MlMetric, EntityType } from '../../../../types.generated';
import { useBaseEntity } from '../../shared/EntityContext';
import { GetMlModelQuery } from '../../../../graphql/mlModel.generated';
import { InfoItem } from '../../shared/components/styled/InfoItem';
import { Link } from 'react-router-dom';

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

const VersionTagContainer = styled.span`
    padding: 2px 8px;
    display: inline-flex;
    align-items: center;
    border-radius: 4px;
    border: 1px solid #d9d9d9;
    color: #595959;
    background: #fafafa;
    margin-right: 8px;
    margin-bottom: 4px;
`;

const JobLink = styled(Link)`
    color: #1890ff;
    &:hover {
        text-decoration: underline;
    }
`;

export default function MLModelSummary() {
    const baseEntity = useBaseEntity<GetMlModelQuery>();
    const model = baseEntity?.mlModel;
    const entityRegistry = useEntityRegistry();

    console.log("model", model);

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

    const formatDate = (timestamp?: number) => {
        if (!timestamp) return '-';
        const milliseconds = timestamp < 10000000000 ? timestamp * 1000 : timestamp;
        return new Date(milliseconds).toISOString().slice(0, 19).replace('T', ' ');
    };

    const renderTrainingJobs = () => {
        const lineageTrainingJobs = model?.properties?.mlModelLineageInfo?.trainingJobs || [];
        console.log("lineageTrainingJobs", model?.properties?.mlModelLineageInfo?.trainingJobs);
        
        if (lineageTrainingJobs.length === 0) return '-';
        
        // TODO: get job name from job URN
        return lineageTrainingJobs.map((jobUrn, index) => (
            <div key={jobUrn}>
                <JobLink to={entityRegistry.getEntityUrl(EntityType.DataProcessInstance, jobUrn)}>
                    {jobUrn}
                </JobLink>
                {index < lineageTrainingJobs.length - 1 && ', '}
            </div>
        ));
    };

    return (
        <TabContent>
            <Space direction="vertical" style={{ width: '100%' }} size="large">
            <Typography.Title level={3}>Model Details</Typography.Title>
                <InfoItemContainer justifyContent="left">
                    {/* TODO: should use versionProperties? */}
                    <InfoItem title="Version">
                        <InfoItemContent>{model?.properties?.version}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Registered At">
                        <InfoItemContent>{formatDate(model?.properties?.created?.time)}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Last Modified At">
                        <InfoItemContent>{formatDate(model?.properties?.lastModified?.time)}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title="Created By">
                        <InfoItemContent>{model?.properties?.created?.actor}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Aliases">
                        <InfoItemContent>
                            {/* use versionProperties for aliases */}
                            {/* {model?.versionProperties?.aliases?.map((alias, index) => (
                                <VersionTagContainer key={`${alias.version}-${index}`}>
                                    {alias.version}
                                </VersionTagContainer>
                            ))} */}
                        </InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Source Run">
                        <InfoItemContent>
                            {renderTrainingJobs()}
                        </InfoItemContent>
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
