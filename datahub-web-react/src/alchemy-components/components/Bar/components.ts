/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

export const BarContainer = styled.div`
    display: flex;
    gap: 2px;
    align-items: baseline;
`;

export const IndividualBar = styled.div<{ height: number; isColored: boolean; color: string; size: string }>`
    width: ${(props) => (props.size === 'default' ? '5px' : '3px')};
    height: ${(props) => props.height}px;
    background-color: ${(props) => (props.isColored ? props.color : '#C6C0E0')};
    border-radius: 20px;
    transition:
        background-color 0.3s ease,
        height 0.3s ease;
`;
