import React from 'react';
import styled from 'styled-components/macro';
import { EntityType } from '../../types.generated';
import LineageExplorer from './LineageExplorer';

const LineageFullscreenWrapper = styled.div`
    background-color: white;
    height: 100%;
    display: flex;
`;

interface Props {
    urn: string;
    type: EntityType;
}

export default function LineageFullscreen({ urn, type }: Props) {
    return (
        <LineageFullscreenWrapper>
            <LineageExplorer urn={urn} type={type} />
        </LineageFullscreenWrapper>
    );
}
