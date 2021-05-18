import { List, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { EntityType, EntityRelationship } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

const DataJobsList = styled(List)`
    padding: 16px 32px;
`;
const DataJobItem = styled(List.Item)`
    padding-top: 20px;
`;

export type Props = {
    dataJobs?: (EntityRelationship | null)[] | null;
};

export default function DataFlowDataJobs({ dataJobs }: Props) {
    const entityRegistry = useEntityRegistry();
    const dataJobsSource = dataJobs?.filter((d) => !!d?.entity).map((d) => d?.entity);
    return (
        <DataJobsList
            bordered
            dataSource={dataJobsSource || []}
            header={<Typography.Title level={3}>DataJobs</Typography.Title>}
            renderItem={(item) => (
                <DataJobItem>{entityRegistry.renderPreview(EntityType.DataJob, PreviewType.PREVIEW, item)}</DataJobItem>
            )}
        />
    );
}
