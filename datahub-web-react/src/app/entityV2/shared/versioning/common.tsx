import { Pill } from '@components';
import { PillProps } from '@components/components/Pills/types';
import React from 'react';
import styled from 'styled-components';

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
