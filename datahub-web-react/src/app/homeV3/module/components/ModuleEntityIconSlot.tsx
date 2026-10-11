import React from 'react';
import styled from 'styled-components';

const Slot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    flex-shrink: 0;
`;

type Props = {
    children: React.ReactNode;
    className?: string;
};

export default function ModuleEntityIconSlot({ children, className }: Props) {
    return <Slot className={className}>{children}</Slot>;
}
