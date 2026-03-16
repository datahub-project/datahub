import React from 'react';
import { Button, Descriptions, Tag, Typography } from 'antd';
import styled from 'styled-components';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import { getStructuredValue } from '@app/entity/shared/tabs/Dataset/Privacy/utils';

const { Text } = Typography;

const Container = styled.div`
    padding: 24px;
`;

type ViewRetentionProps = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    structuredProps: any[];
    onEdit: () => void;
};

export function ViewRetention({ structuredProps, onEdit }: ViewRetentionProps) {
    const retentionColumn = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.RetentionColumn);
    const retentionDays = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.RetentionDays);
    const retentionJira = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.RetentionJira);

    return (
        <Container>
            <Button type="primary" onClick={onEdit}>
                Edit
            </Button>

            <Descriptions bordered column={1}>
                <Descriptions.Item label="Retention Column">
                    {retentionColumn || <Text type="secondary">—</Text>}
                </Descriptions.Item>
                <Descriptions.Item label="Retention Days">
                    {retentionDays ? <Tag color="purple">{retentionDays} days</Tag> : <Text type="secondary">—</Text>}
                </Descriptions.Item>
                <Descriptions.Item label="Retention Exception Jira">
                    {retentionJira || <Text type="secondary">—</Text>}
                </Descriptions.Item>
            </Descriptions>
        </Container>
    );
}

export default ViewRetention;
