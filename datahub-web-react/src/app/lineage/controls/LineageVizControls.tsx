import React from 'react';
import styled from 'styled-components/macro';

import { LINEAGE_GRAPH_TIME_FILTER_ID } from '../../onboarding/config/LineageGraphOnboardingConfig';
import LineageVizTimeSelector from './LineageVizTimeSelector';
import { useIsSeparateSiblingsMode } from '../../entity/shared/siblingUtils';
import { useIsShowColumnsMode } from '../utils/useIsShowColumnsMode';
import { LineageVizToggles } from './LineageVizToggles';

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
