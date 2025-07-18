import React from 'react';
import styled from 'styled-components';

const DepthMarginContainer = styled.div<{ $depth: number }>`
    margin-left: calc(8px * ${(props) => props.$depth});
`;

interface Props {
    depth: number;
}

export default function DepthMargin({ depth }: Props) {
    return <DepthMarginContainer $depth={depth} />;
}
