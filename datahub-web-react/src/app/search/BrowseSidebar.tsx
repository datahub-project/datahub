import React, { memo } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { ANTD_GRAY } from '../entity/shared/constants';

const Sidebar = styled.div<{ visible: boolean; width: number }>`
    height: 100%;
    width: ${(props) => (props.visible ? `${props.width}px` : '0')};
    transition: width 0.25s ease-in-out;
    border-right: 1px solid ${(props) => props.theme.styles['border-color-base']};
    background-color: ${ANTD_GRAY[2]};
`;

const SidebarHeader = styled.div`
    display: flex;
    align-items: center;
    padding-left: 24px;
    height: 47px;
    border-bottom: 1px solid ${(props) => props.theme.styles['border-color-base']};
    white-space: nowrap;
`;

const SidebarBody = styled.div`
    padding-left: 24px;
    white-space: nowrap;
`;

const BrowseSidebar = ({ visible, width }: { visible: boolean; width: number }) => {
    return (
        <Sidebar visible={visible} width={width}>
            <SidebarHeader>
                <Typography.Text strong>Navigate</Typography.Text>
            </SidebarHeader>
            <SidebarBody>TBD</SidebarBody>
        </Sidebar>
    );
};

export default memo(BrowseSidebar);
