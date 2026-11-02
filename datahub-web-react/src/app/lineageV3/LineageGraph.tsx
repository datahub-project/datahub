import React from 'react';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import LineageExplorer from '@app/lineageV3/LineageExplorer';

const LineageFullscreenWrapper = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
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
    const explorer = <LineageExplorer urn={lineageUrn} type={entityType} />;
    if (isFullscreen) {
        return <LineageFullscreenWrapper>{explorer}</LineageFullscreenWrapper>;
    }
    return explorer;
}
