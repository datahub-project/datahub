import React from 'react';
import styled from 'styled-components';
import { Tooltip } from 'antd';

const HeaderText = styled.div`
    margin-bottom: 8px;
`;

type Props = {
    children: React.ReactNode;
    visible?: boolean;
};

export const ViewSelectToolTip = ({ children, visible = true }: Props) => {
    return (
        <Tooltip
            overlayStyle={{ display: !visible ? 'none' : undefined }}
            placement="right"
            title={
                <>
                    <HeaderText>Select a View to apply to search results.</HeaderText>
                    <div>Views help narrow down search results to those that matter most to you.</div>
                </>
            }
        >
            {children}
        </Tooltip>
    );
};
