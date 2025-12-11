/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

const CircleContainer = styled.span<{ size?: number }>`
    width: ${(props) => (props.size ? props.size : 10)}px;
    height: ${(props) => (props.size ? props.size : 10)}px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const Circle = styled.span<{ color: string; size?: number }>`
    width: ${(props) => (props.size ? props.size : 6)}px;
    height: ${(props) => (props.size ? props.size : 6)}px;
    border-radius: 50%;
    padding: 0px;
    margin: 0px;
    background-color: ${(props) => props.color};
    opacity: 1;
`;

type Props = {
    title?: React.ReactNode;
    color: string;
    size?: number;
};

export const DefaultViewIcon = ({ title, color, size }: Props) => {
    return (
        <Tooltip title={title}>
            <CircleContainer size={size}>
                <Circle color={color} size={size} />
            </CircleContainer>
        </Tooltip>
    );
};
