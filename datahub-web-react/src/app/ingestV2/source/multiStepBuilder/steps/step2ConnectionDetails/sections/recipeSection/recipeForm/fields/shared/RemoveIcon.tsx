import { Icon } from '@components';
import React from 'react';
import styled from 'styled-components';

const StyledIcon = styled(Icon)`
    flex-shrink: 0;

    &:hover {
        cursor: pointer;
    }
`;

interface Props {
    onClick?: () => void;
    className?: string;
}

export function RemoveIcon({ onClick, className }: Props) {
    return <StyledIcon source="phosphor" icon="Trash" onClick={onClick} size="lg" color="red" className={className} />;
}
