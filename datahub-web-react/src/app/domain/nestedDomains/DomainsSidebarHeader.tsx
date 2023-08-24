import { PlusOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY, ANTD_GRAY_V2 } from '../../entity/shared/constants';
import DomainsTitle from './DomainsTitle';

const HeaderWrapper = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4]};
    padding: 16px;
    font-size: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const StyledButton = styled(Button)`
    box-shadow: none;
    border-color: ${ANTD_GRAY_V2[6]};
`;

export default function DomainsSidebarHeader() {
    return (
        <HeaderWrapper>
            <DomainsTitle />
            {/* TODO - give functionality to this button */}
            <StyledButton icon={<PlusOutlined />} />
        </HeaderWrapper>
    );
}
