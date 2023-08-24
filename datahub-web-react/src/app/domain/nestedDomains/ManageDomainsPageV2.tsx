import { Button } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components/macro';
import DomainsTitle from './DomainsTitle';

const PageWrapper = styled.div`
    background-color: #f8f9fa;
    flex: 1;
`;

const Header = styled.div`
    display: flex;
    justify-content: space-between;
    padding: 32px 24px;
    font-size: 30px;
    align-items: center;
`;

export default function ManageDomainsPageV2() {
    return (
        <PageWrapper>
            <Header>
                <DomainsTitle />
                <Button type="primary">
                    <PlusOutlined /> New Domain
                </Button>
            </Header>
        </PageWrapper>
    );
}
