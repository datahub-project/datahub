import React from 'react';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import LineageExplorerV2 from '@app/lineageV2/LineageExplorer';
import LineageExplorerV3 from '@app/lineageV3/LineageExplorer';
import { useAppConfig } from '@app/useAppConfig';

import { EntityType } from '@types';

const LineageFullscreenWrapper = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
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
    // DataFlow, DataProduct and Domain have V3-only initializers (the V2 explorer does not know
    // how to build a member-pass-through or aggregated-edge graph for them), so they always
    // render with the V3 explorer.
    const explorer =
        lineageGraphV3 ||
        entityType === EntityType.DataFlow ||
        entityType === EntityType.DataProduct ||
        entityType === EntityType.Domain ? (
            <LineageExplorerV3 {...props} />
        ) : (
            <LineageExplorerV2 {...props} />
        );
    if (isFullscreen) {
        return <LineageFullscreenWrapper>{explorer}</LineageFullscreenWrapper>;
    }
    return explorer;
}
