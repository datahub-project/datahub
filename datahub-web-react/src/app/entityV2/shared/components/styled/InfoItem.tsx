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

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

const HeaderInfoItem = styled.div<{ onClick?: () => void; width?: string }>`
    display: inline-block;
    text-align: left;
    width: ${(props) => (props.width ? `${props.width};` : '125px;')};
    vertical-align: top;
    &:hover {
        cursor: ${(props) => (props.onClick ? 'pointer' : 'default')};
    }
`;

const HeaderInfoTitle = styled(Typography.Text)`
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    title: string;
    children: React.ReactNode;
    onClick?: () => void;
    width?: string;
};

export const InfoItem = ({ title, children, width, onClick }: Props) => {
    return (
        <HeaderInfoItem onClick={onClick} width={width}>
            <div>
                <HeaderInfoTitle>{title}</HeaderInfoTitle>
            </div>
            <span>{children}</span>
        </HeaderInfoItem>
    );
};
