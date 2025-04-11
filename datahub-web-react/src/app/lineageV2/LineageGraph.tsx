import { useEntityData } from '@app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import LineageExplorerV2 from '@app/lineageV2/LineageExplorer';
import React from 'react';
import styled from 'styled-components/macro';

const LineageFullscreenWrapper = styled.div`
    background-color: white;
    height: 100%;
    display: flex;
`;

interface Props {
    isFullscreen?: boolean;
}

export default function LineageGraph({ isFullscreen }: Props) {
    const { urn, entityType, entityData } = useEntityData();
    const onIndividualSiblingPage = useIsSeparateSiblingsMode();

    const lineageUrn = (!onIndividualSiblingPage && entityData?.lineageUrn) || urn;
    const explorer = <LineageExplorerV2 urn={lineageUrn} type={entityType} />;
    if (isFullscreen) {
        return <LineageFullscreenWrapper>{explorer}</LineageFullscreenWrapper>;
    }
    return explorer;
}
