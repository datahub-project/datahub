import React, { useEffect } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';
import { EntityType } from '../../types.generated';
import { getEntityPath } from '../entityV2/shared/containers/profile/utils';
import { useEntityRegistry } from '../useEntityRegistry';
import LineageExplorer from './LineageExplorer';

const LineageFullscreenWrapper = styled.div`
    background-color: white;
    height: calc(100vh - 60px);
    display: flex;
`;

interface Props {
    urn: string;
    type: EntityType;
}

export default function LineageFullscreen({ urn, type }: Props) {
    useSetEmbedded(urn, type);

    return (
        <LineageFullscreenWrapper>
            <LineageExplorer urn={urn} type={type} />
        </LineageFullscreenWrapper>
    );
}

function useSetEmbedded(urn: string, type: EntityType) {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();

    function handleKeyPress(e: KeyboardEvent) {
        if (e.key === 'f') {
            history.push(getEntityPath(type, urn, entityRegistry, false, false, 'Lineage'));
        }
    }

    useEffect(() => {
        document.addEventListener('keydown', handleKeyPress);
        return () => {
            document.removeEventListener('keydown', handleKeyPress);
        };
    });
}
