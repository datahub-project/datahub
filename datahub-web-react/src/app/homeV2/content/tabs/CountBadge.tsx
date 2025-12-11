/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

const DEFAULT_BADGE_COLOR = '#3CB47A';

const Badge = styled.div<{ color: string }>`
    height: 16px;
    width: 16px;
    border-radius: 50%;
    background-color: ${(props) => props.color};
    font-size: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #ffffff;
    margin-left: -4px;
    margin-top: -12px;
    border: 1px solid #ffffff;
`;

type Props = {
    count: number;
    color?: string;
};

export const CountBadge = ({ count, color = DEFAULT_BADGE_COLOR }: Props) => {
    if (!count) {
        return null;
    }
    return <Badge color={color}>{count}</Badge>;
};
