import { Icon } from '@components';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
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
    return <StyledIcon icon={Trash} onClick={onClick} size="lg" color="red" className={className} />;
}
