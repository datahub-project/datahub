import { PlusOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

import { Card } from '@app/sharedV2/cards/components';

const DataProductTitle = styled.div`
    font-size: 16px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textInformation};
    padding: 10px 14px;
`;

export default function AddOutputPortCard() {
    return (
        <Card>
            <DataProductTitle>
                <PlusOutlined style={{ marginRight: 4 }} />
                Add Output Port
            </DataProductTitle>
        </Card>
    );
}
