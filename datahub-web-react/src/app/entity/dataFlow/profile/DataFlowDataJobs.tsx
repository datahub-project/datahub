import { List, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { DataJob, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

const DataJobsList = styled(List)`
    margin-top: 12px;
    padding: 16px 32px;
`;
const DataJobItem = styled(List.Item)`
    padding-top: 20px;
`;

export type Props = {
    dataJobs?: DataJob[] | null;
};

export default function DataFlowDataJobs({ dataJobs }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <DataJobsList
            bordered
            dataSource={dataJobs || []}
            header={<Typography.Title level={3}>DataJobs</Typography.Title>}
            renderItem={(item) => (
                <DataJobItem>{entityRegistry.renderPreview(EntityType.DataJob, PreviewType.PREVIEW, item)}</DataJobItem>
            )}
        />
    );
}
