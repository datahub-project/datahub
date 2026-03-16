import React from 'react';
import { Button, Descriptions, Tag, Typography } from 'antd';
import styled from 'styled-components';

import { CompliancePropertyQualifiedName } from '@app/entity/shared/tabs/Dataset/Privacy/constant';
import { getStructuredValue } from '@app/entity/shared/tabs/Dataset/Privacy/utils';

const { Text } = Typography;

const Container = styled.div`
    padding: 24px;
`;

type ViewScrubbingProps = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    structuredProps: any[];
    onEdit: () => void;
};

export function ViewScrubbing({ structuredProps, onEdit }: ViewScrubbingProps) {
    const scrubbingOp = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.ScrubbingOp);
    const scrubbingState = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.ScrubbingState);
    const scrubbingStatus = getStructuredValue(structuredProps, CompliancePropertyQualifiedName.ScrubbingStatus);

    return (
        <Container>
            <Button type="primary" onClick={onEdit}>
                Edit
            </Button>

            <Descriptions bordered column={1}>
                <Descriptions.Item label="Scrubbing Operation">
                    {scrubbingOp || <Text type="secondary">—</Text>}
                </Descriptions.Item>
                <Descriptions.Item label="Scrubbing State">
                    {scrubbingState || <Text type="secondary">—</Text>}
                </Descriptions.Item>
                <Descriptions.Item label="Scrubbing Status">
                    {scrubbingStatus ? <Tag color="cyan">{scrubbingStatus}</Tag> : <Text type="secondary">—</Text>}
                </Descriptions.Item>
            </Descriptions>
        </Container>
    );
}

export default ViewScrubbing;