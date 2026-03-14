import React from 'react';
import { Button, Descriptions, Space, Tag, Typography } from 'antd';
import styled from 'styled-components';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import { getStructuredList } from '@app/entity/shared/tabs/Dataset/Privacy/utils';

const { Text } = Typography;

const Container = styled.div`
    padding: 24px;
`;

type ViewRecordsClassProps = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    structuredProps: any[];
    onEdit: () => void;
};

export function ViewRecordsClass({ structuredProps, onEdit }: ViewRecordsClassProps) {
    const recordsClasses = getStructuredList(structuredProps, CompliancePropertyQualifiedName.RecordsClass);

    return (
        <Container>
            <Button type="primary" onClick={onEdit}>
                Edit
            </Button>

            <Descriptions bordered column={1}>
                <Descriptions.Item label="Records Class">
                    {recordsClasses.length > 0 ? (
                        <Space size={[0, 4]} wrap>
                            {recordsClasses.map((cls) => (
                                <Tag key={cls} color="blue">
                                    {cls}
                                </Tag>
                            ))}
                        </Space>
                    ) : (
                        <Text type="secondary">None</Text>
                    )}
                </Descriptions.Item>
            </Descriptions>
        </Container>
    );
}

export default ViewRecordsClass;
