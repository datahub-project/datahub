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

import { useIsSeparateSiblingsMode } from '@app/entity/shared/siblingUtils';
import LineageVizTimeSelector from '@app/lineage/controls/LineageVizTimeSelector';
import { LineageVizToggles } from '@app/lineage/controls/LineageVizToggles';
import { useIsShowColumnsMode } from '@app/lineage/utils/useIsShowColumnsMode';
import { LINEAGE_GRAPH_TIME_FILTER_ID } from '@app/onboarding/config/LineageGraphOnboardingConfig';

const LeftControlsDiv = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-evenly;
    gap: 30px;
    margin-left: 48px;
`;

const RightControlsDiv = styled.div`
    display: flex;
    flex-direction: column;
    margin-right: 48px;
`;

type Props = {
    showExpandedTitles: boolean;
    setShowExpandedTitles: (showExpandedTitles: boolean) => void;
};

export function LineageVizControls({ showExpandedTitles, setShowExpandedTitles }: Props) {
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const showColumns = useIsShowColumnsMode();

    return (
        <>
            <LeftControlsDiv>
                <LineageVizToggles
                    showExpandedTitles={showExpandedTitles}
                    setShowExpandedTitles={setShowExpandedTitles}
                />
            </LeftControlsDiv>
            <RightControlsDiv>
                <span id={LINEAGE_GRAPH_TIME_FILTER_ID}>
                    <LineageVizTimeSelector isHideSiblingMode={isHideSiblingMode} showColumns={showColumns} />
                </span>
            </RightControlsDiv>
        </>
    );
}
