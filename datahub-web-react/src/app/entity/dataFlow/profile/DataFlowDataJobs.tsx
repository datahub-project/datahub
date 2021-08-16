import { List, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { EntityType, EntityRelationshipLegacy } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

const DataJobsList = styled(List)`
    padding: 0;
    border: none;
    && .ant-list-header {
        padding-left: 0;
        padding-top: 0;
        padding-bottom: 24px;
    }
`;
const DataJobItem = styled(List.Item)`
    padding-top: 20px;
    border-left: 1px solid #f0f0f0;
    border-right: 1px solid #f0f0f0;
    &&:last-child {
        border-bottom: 1px solid #f0f0f0;
    }
`;

export type Props = {
    dataJobs?: (EntityRelationshipLegacy | null)[] | null;
};

export default function DataFlowDataJobs({ dataJobs }: Props) {
    const entityRegistry = useEntityRegistry();
    const dataJobsSource = dataJobs?.filter((d) => !!d?.entity).map((d) => d?.entity);
    return (
        <DataJobsList
            bordered
            dataSource={dataJobsSource || []}
            header={
                <Typography.Title level={3}>{entityRegistry.getCollectionName(EntityType.DataJob)}</Typography.Title>
            }
            renderItem={(item) => (
                <DataJobItem>{entityRegistry.renderPreview(EntityType.DataJob, PreviewType.PREVIEW, item)}</DataJobItem>
            )}
            data-testid="dataflow-jobs-list"
        />
    );
}
