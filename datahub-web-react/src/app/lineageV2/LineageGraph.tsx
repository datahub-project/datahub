import React from 'react';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import LineageExplorerV2 from '@app/lineageV2/LineageExplorer';
import LineageExplorerV3 from '@app/lineageV3/LineageExplorer';
import { useAppConfig } from '@app/useAppConfig';

const LineageFullscreenWrapper = styled.div`
    background-color: white;
    height: 100%;
    display: flex;
`;

interface Props {
    isFullscreen?: boolean;
}

export default function LineageGraph({ isFullscreen }: Props) {
    const {
        config: {
            featureFlags: { lineageGraphV3 },
        },
    } = useAppConfig();
    const { urn, entityType, entityData } = useEntityData();
    const onIndividualSiblingPage = useIsSeparateSiblingsMode();

    const lineageUrn = (!onIndividualSiblingPage && entityData?.lineageUrn) || urn;
    const props = { urn: lineageUrn, type: entityType };
    const explorer = lineageGraphV3 ? <LineageExplorerV3 {...props} /> : <LineageExplorerV2 {...props} />;
    if (isFullscreen) {
        return <LineageFullscreenWrapper>{explorer}</LineageFullscreenWrapper>;
    }
    return explorer;
}
