/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

const HeaderContainer = styled.div`
    min-height: 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 8px;
    > .ant-typography {
        margin-bottom: 0;
    }
`;

type Props = {
    title: string;
    actions?: React.ReactNode;
    children?: React.ReactNode;
};

export const SidebarHeader = ({ title, actions, children }: Props) => {
    return (
        <HeaderContainer>
            <Typography.Title level={5}>{title}</Typography.Title>
            {actions && <div>{actions}</div>}
            {children}
        </HeaderContainer>
    );
};
