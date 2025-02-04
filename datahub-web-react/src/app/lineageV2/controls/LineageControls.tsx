import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { useGetLineageTimeParams } from '@app/lineage/utils/useGetLineageTimeParams';
import React, { useContext, useEffect, useState } from 'react';
import { Panel, useReactFlow } from 'reactflow';
import styled from 'styled-components';
import {
    ArrowsAltOutlined,
    CalendarOutlined,
    FilterOutlined,
    HomeOutlined,
    ShrinkOutlined,
    VerticalLeftOutlined,
} from '@ant-design/icons';
import { Button, Divider } from 'antd';
import { LineageNodesContext, TRANSITION_DURATION_MS } from '../common';

import LineageSearchFilters from './LineageSearchFilters';
import { StyledPanelButton } from './StyledPanelButton';
import DownloadLineageScreenshotButton from './DownloadLineageScreenshotButton';
import LineageTimeRangeControls from './LineageTimeRangeControls';
import TabFullsizedContext from '../../shared/TabFullsizedContext';
import { ControlPanel } from './common';

const StyledPanel = styled(Panel)`
    margin-top: 80px;
    display: flex;
    flex-direction: row;
    gap: 10px;
    height: 0; // Allow pointer events in gaps
`;

const StyledControlsPanel = styled(ControlPanel)<{ isExpanded: boolean }>`
    padding: 2px;
    width: ${({ isExpanded }) => (isExpanded ? '150px' : '50px')};
    transition: width ${TRANSITION_DURATION_MS}ms ease-in-out;
`;

const StyledExpandContractButton = styled(Button)`
    border-radius: 8px;
    height: 56px;
    width: 56px;
    margin-top: 8px;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    display: flex;
`;

const StyledDivider = styled(Divider)`
    margin-top: 1px;
    margin-bottom: 1px;
`;

const ControlsColumn = styled.div``;

type PanelType = 'filters' | 'timeRange';

export default function LineageControls() {
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
            setShowExpandedText(true);
            return () => {};
        }
        const timeout = setTimeout(() => {
            setShowExpandedText(false);
        }, TRANSITION_DURATION_MS);
        return () => clearTimeout(timeout);
    }, [isExpanded]);

    return (
        <StyledPanel position="top-left">
            <ControlsColumn>
                <StyledControlsPanel isExpanded={isExpanded}>
                    <StyledPanelButton type="text" onClick={() => setIsExpanded(!isExpanded)}>
                        <VerticalLeftOutlined rotate={isExpanded ? 180 : 0} />
                        {showExpandedText ? 'Hide Menu' : null}
                    </StyledPanelButton>
                    <StyledDivider />
                    <StyledPanelButton
                        type="text"
                        onClick={() => {
                            fitView({ duration: 1000, nodes: [{ id: rootUrn }], maxZoom: 1 });
                        }}
                    >
                        <HomeOutlined />
                        {showExpandedText ? 'Focus on Home' : null}
                    </StyledPanelButton>
                    <StyledDivider />
                    <StyledPanelButton
                        type="text"
                        onClick={() =>
                            visiblePanel === 'filters' ? setVisiblePanel(null) : setVisiblePanel('filters')
                        }
                    >
                        <FilterOutlined
                            style={{
                                color:
                                    hideTransformations || showDataProcessInstances || showGhostEntities
                                        ? REDESIGN_COLORS.BLUE
                                        : undefined,
                            }}
                        />
                        {showExpandedText ? 'Filter' : null}
                    </StyledPanelButton>
                    <StyledPanelButton
                        type="text"
                        onClick={() =>
                            visiblePanel === 'timeRange' ? setVisiblePanel(null) : setVisiblePanel('timeRange')
                        }
                    >
                        <CalendarOutlined
                            style={{ color: isLineageTimeUnchanged ? undefined : REDESIGN_COLORS.BLUE }}
                        />
                        {showExpandedText ? 'Time Range' : null}
                    </StyledPanelButton>
                    <StyledDivider />
                    <DownloadLineageScreenshotButton showExpandedText={showExpandedText} />
                </StyledControlsPanel>
                {setTabFullsize && (
                    <StyledExpandContractButton onClick={() => setTabFullsize((v) => !v)}>
                        {isTabFullsize ? (
                            <ShrinkOutlined style={{ fontSize: '150%' }} />
                        ) : (
                            <ArrowsAltOutlined style={{ fontSize: '150%' }} />
                        )}
                    </StyledExpandContractButton>
                )}
            </ControlsColumn>
            {visiblePanel === 'filters' && <LineageSearchFilters />}
            {visiblePanel === 'timeRange' && <LineageTimeRangeControls />}
        </StyledPanel>
    );
}
