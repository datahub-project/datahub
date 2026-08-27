import React from 'react';
import styled from 'styled-components';

const Row = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
`;

type Props = {
    children: React.ReactNode;
    className?: string;
};

export default function ModulePillRow({ children, className }: Props) {
    return <Row className={className}>{children}</Row>;
}
