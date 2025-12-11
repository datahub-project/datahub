/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Card } from 'antd';
import styled from 'styled-components';

export const ChartCard = styled(Card)<{ $shouldScroll: boolean }>`
    margin: 12px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    height: 440px;
    overflow-y: ${(props) => (props.$shouldScroll ? 'scroll' : 'hidden')};
`;
