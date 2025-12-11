/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Pill } from '@components';
import React from 'react';
import styled from 'styled-components';

import { PillProps } from '@components/components/Pills/types';

const StyledPill = styled(Pill)`
    font-weight: 600;
    line-height: 1.4;
`;

interface Props {
    isLatest?: boolean;
}

export function VersionPill(props: Props & PillProps) {
    return <StyledPill variant="version" clickable={false} color={props.isLatest ? 'white' : undefined} {...props} />;
}
