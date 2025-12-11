/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import LineageExplorerV2 from '@app/lineageV2/LineageExplorer';
import LineageExplorerV3 from '@app/lineageV3/LineageExplorer';
import { useAppConfig } from '@app/useAppConfig';

import { EntityType } from '@types';

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
    const explorer =
        lineageGraphV3 || entityType === EntityType.DataFlow ? (
            <LineageExplorerV3 {...props} />
        ) : (
            <LineageExplorerV2 {...props} />
        );
    if (isFullscreen) {
        return <LineageFullscreenWrapper>{explorer}</LineageFullscreenWrapper>;
    }
    return explorer;
}
