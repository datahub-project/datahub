/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

const CircleContainer = styled.span`
    width: 10px;
    height: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const Circle = styled.span<{ color: string }>`
    width: 6px;
    height: 6px;
    border-radius: 50%;
    padding: 0px;
    margin: 0px;
    background-color: ${(props) => props.color};
    opacity: 1;
`;

type Props = {
    title?: React.ReactNode;
    color: string;
};

export const DefaultViewIcon = ({ title, color }: Props) => {
    return (
        <Tooltip title={title}>
            <CircleContainer>
                <Circle color={color} />
            </CircleContainer>
        </Tooltip>
    );
};
