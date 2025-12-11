/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Icon, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

const StyledIcon = styled(Icon)`
    flex-shrink: 0;
    margin: 0 2px;
    color: ${colors.gray[200]};
`;

export default function ContextPathSeparator() {
    return <StyledIcon icon="CaretRight" source="phosphor" size="sm" />;
}
