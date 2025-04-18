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
