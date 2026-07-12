import { ArrowsIn } from '@phosphor-icons/react/dist/csr/ArrowsIn';
import { ArrowsOut } from '@phosphor-icons/react/dist/csr/ArrowsOut';
import { CalendarBlank } from '@phosphor-icons/react/dist/csr/CalendarBlank';
import { Funnel } from '@phosphor-icons/react/dist/csr/Funnel';
import { House } from '@phosphor-icons/react/dist/csr/House';
import { SidebarSimple } from '@phosphor-icons/react/dist/csr/SidebarSimple';
import React, { useContext, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Panel, useReactFlow } from 'reactflow';
import styled from 'styled-components';

import LineageControlIcon from '@app/lineage/controls/LineageControlIcon';
import { isLineageFilterActive } from '@app/lineage/controls/lineageControlsUtils';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import { LineageNodesContext, TRANSITION_DURATION_MS } from '@app/lineageV2/common';
import DownloadLineageScreenshotButton from '@app/lineageV2/controls/DownloadLineageScreenshotButton';
import LineageSearchFilters from '@app/lineageV2/controls/LineageSearchFilters';
import LineageTimeRangeControls from '@app/lineageV2/controls/LineageTimeRangeControls';
import { StyledPanelButton } from '@app/lineageV2/controls/StyledPanelButton';
import { ControlPanel } from '@app/lineageV2/controls/common';
import TabFullsizedContext from '@app/shared/TabFullsizedContext';

const StyledPanel = styled(Panel)`
    margin-top: 80px;
    display: flex;
    flex-direction: row;
    gap: 10px;
    height: 0; // Allow pointer events in gaps
`;

const StyledControlsPanel = styled(ControlPanel)<{ isExpanded: boolean }>`
    padding: 2px;
    width: ${({ isExpanded }) => (isExpanded ? '200px' : '50px')};
    transition: width ${TRANSITION_DURATION_MS}ms ease-in-out;
`;

const StyledDivider = styled.div`
    margin-top: 1px;
    margin-bottom: 1px;
    height: 1px;
    background-color: ${(props) => props.theme.colors.border};
`;

const ControlsColumn = styled.div``;

type PanelType = 'filters' | 'timeRange';

export default function LineageControls() {
    const { t } = useTranslation('lineage');
    const { rootUrn, hideTransformations, showDataProcessInstances, showGhostEntities } =
        useContext(LineageNodesContext);
    const { isTabFullsize, setTabFullsize } = useContext(TabFullsizedContext);
    const { isDefault: isLineageTimeUnchanged } = useGetLineageTimeParams();
    const { fitView } = useReactFlow();

    const [isExpanded, setIsExpanded] = useState(false);
    const [visiblePanel, setVisiblePanel] = useState<PanelType | null>(null);

    // showExpandedText is a delayed version of isExpanded by .3 seconds
    const [showExpandedText, setShowExpandedText] = useState(false);
    useEffect(() => {
        if (isExpanded) {
            const timeout = setTimeout(() => {
                setShowExpandedText(true);
            }, TRANSITION_DURATION_MS);
            return () => clearTimeout(timeout);
        }
        setShowExpandedText(false);
        return () => {};
    }, [isExpanded]);

    const isFilterActive = isLineageFilterActive({ hideTransformations, showDataProcessInstances, showGhostEntities });

    return (
        <StyledPanel position="top-left">
            <ControlsColumn>
                <StyledControlsPanel isExpanded={isExpanded}>
                    <StyledPanelButton $showText={isExpanded} onClick={() => setIsExpanded(!isExpanded)}>
                        <LineageControlIcon icon={SidebarSimple} rotate={isExpanded ? '180' : '0'} color="icon" />
                        {showExpandedText ? t('controls.hideMenu.label') : null}
                    </StyledPanelButton>
                    <StyledDivider />
                    <StyledPanelButton
                        $showText={isExpanded}
                        onClick={() => {
                            fitView({ duration: 1000, nodes: [{ id: rootUrn }], maxZoom: 1 });
                        }}
                    >
                        <LineageControlIcon icon={House} color="icon" />
                        {showExpandedText ? t('controls.focusOnHome.label') : null}
                    </StyledPanelButton>
                    <StyledDivider />
                    <StyledPanelButton
                        $showText={isExpanded}
                        onClick={() =>
                            visiblePanel === 'filters' ? setVisiblePanel(null) : setVisiblePanel('filters')
                        }
                    >
                        <LineageControlIcon icon={Funnel} color={isFilterActive ? 'iconSelected' : 'icon'} />
                        {showExpandedText ? t('controls.filter.label') : null}
                    </StyledPanelButton>
                    <StyledPanelButton
                        $showText={isExpanded}
                        onClick={() =>
                            visiblePanel === 'timeRange' ? setVisiblePanel(null) : setVisiblePanel('timeRange')
                        }
                    >
                        <LineageControlIcon
                            icon={CalendarBlank}
                            color={isLineageTimeUnchanged ? 'icon' : 'iconBrand'}
                        />
                        {showExpandedText ? t('controls.timeRangeLabel') : null}
                    </StyledPanelButton>
                    <StyledDivider />
                    <DownloadLineageScreenshotButton showExpandedText={showExpandedText} isExpanded={isExpanded} />
                    {setTabFullsize && (
                        <>
                            <StyledDivider />
                            <StyledPanelButton $showText={isExpanded} onClick={() => setTabFullsize((v) => !v)}>
                                <LineageControlIcon icon={isTabFullsize ? ArrowsIn : ArrowsOut} color="icon" />
                                {showExpandedText &&
                                    (isTabFullsize ? t('controls.contractView.label') : t('controls.expandView.label'))}
                            </StyledPanelButton>
                        </>
                    )}
                </StyledControlsPanel>
            </ControlsColumn>
            {visiblePanel === 'filters' && <LineageSearchFilters />}
            {visiblePanel === 'timeRange' && <LineageTimeRangeControls />}
        </StyledPanel>
    );
}
