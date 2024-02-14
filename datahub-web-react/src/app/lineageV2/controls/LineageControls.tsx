import React, { useContext, useEffect } from 'react';
import { Panel, useReactFlow, useStoreApi } from 'reactflow';
import styled from 'styled-components';
import {
    ArrowsAltOutlined,
    // CalendarOutlined,
    // CompressOutlined,
    // FilterOutlined,
    HomeOutlined,
    ShrinkOutlined,
    VerticalLeftOutlined,
} from '@ant-design/icons';
import { Button, Divider } from 'antd';

import LineageSearchFilters from './LineageSearchFilters';
import { StyledPanelButton } from './StyledPanelButton';
import DownloadLineageScreenshotButton from './DownloadLineageScreenshotButton';
import LineageTimeRangeControls from './LineageTimeRangeControls';
import TabFullsizedContext from '../../shared/TabFullsizedContext';

const StyledControlsPanel = styled.div<{ isExpanded: boolean }>`
    margin-top: 36px;
    display: flex;
    flex-direction: column;
    align-items: ${({ isExpanded }) => (isExpanded ? 'flex-start' : 'flex-start')};
    padding: 2px;
    width: ${({ isExpanded }) => (isExpanded ? '168px' : '56px')};
    border-radius: 8px;
    border: 1px solid #d5d5d5;
    background: #fff;
    box-shadow: 0px 4px 4px 0px rgba(224, 224, 224, 0.25);
    transition: width 0.3s ease-in-out;
    overflow: hidden;
`;

const StyledExpandContractButton = styled(Button)`
    border-radius: 8px;
    border: 1px solid var(--colorBorder, rgba(0, 0, 0, 0.15));
    background: var(--colorBgContainer, #fff);
    box-shadow: 0px 2px 0px 0px rgba(0, 0, 0, 0.02);
    display: block;
    height: 56px;
    width: 56px;
    margin-top: 8px;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    display: flex;
`;

const StyledDivider = styled(Divider)`
    margin-top: 6px;
    margin-bottom: 6px;
`;

const LineageControls: React.FC = () => {
    const { isTabFullsize, setTabFullsize } = useContext(TabFullsizedContext);

    const [isExpanded, setIsExpanded] = React.useState(false);
    const [visiblePanel] = React.useState<string | null>(null);
    const store = useStoreApi();

    const { setCenter, zoomTo } = useReactFlow();
    // showExpandedText is a delayed version of isExpanded by .3 seconds
    const [showExpandedText, setShowExpandedText] = React.useState(false);
    useEffect(() => {
        if (isExpanded) {
            setShowExpandedText(true);
        } else {
            setTimeout(() => {
                setShowExpandedText(false);
            }, 300);
        }
    }, [isExpanded]);

    return (
        <>
            <Panel position="top-left">
                <StyledControlsPanel isExpanded={isExpanded}>
                    <StyledPanelButton type="text" onClick={() => setIsExpanded(!isExpanded)}>
                        <VerticalLeftOutlined rotate={isExpanded ? 180 : 0} />
                        {showExpandedText ? 'Hide Menu' : null}
                    </StyledPanelButton>
                    <StyledDivider />
                    <StyledPanelButton
                        type="text"
                        onClick={() => {
                            setCenter(0, 0);
                            zoomTo(1);
                        }}
                    >
                        <HomeOutlined />
                        {showExpandedText ? 'Go to home node' : null}
                    </StyledPanelButton>
                    {/* <StyledPanelButton */}
                    {/*     type="text" */}
                    {/*     onClick={() => */}
                    {/*         visiblePanel === 'filters' ? setVisiblePanel(null) : setVisiblePanel('filters') */}
                    {/*     } */}
                    {/* > */}
                    {/*     <FilterOutlined /> */}
                    {/*     {showExpandedText ? 'Filters' : null} */}
                    {/* </StyledPanelButton> */}
                    {/* <StyledPanelButton */}
                    {/*     type="text" */}
                    {/*     onClick={() => */}
                    {/*         visiblePanel === 'timeRange' ? setVisiblePanel(null) : setVisiblePanel('timeRange') */}
                    {/*     } */}
                    {/* > */}
                    {/*     <CalendarOutlined /> */}
                    {/*     {showExpandedText ? 'Date Range' : null} */}
                    {/* </StyledPanelButton> */}
                    <StyledDivider />
                    {/* <StyledPanelButton type="text"> */}
                    {/*     <CompressOutlined /> */}
                    {/*     {showExpandedText ? 'Compress Lineage' : null} */}
                    {/* </StyledPanelButton> */}
                    <DownloadLineageScreenshotButton showExpandedText={showExpandedText} />
                    {visiblePanel === 'filters' && <LineageSearchFilters isRootPanelExpanded={isExpanded} />}
                    {visiblePanel === 'timeRange' && <LineageTimeRangeControls isRootPanelExpanded={isExpanded} />}
                </StyledControlsPanel>
                <StyledExpandContractButton
                    onClick={() => {
                        if (!isTabFullsize) {
                            setTabFullsize(true);
                            store.getState().resetSelectedElements();
                        } else {
                            setTabFullsize(false);
                        }
                    }}
                >
                    {isTabFullsize ? (
                        <ShrinkOutlined style={{ fontSize: '150%' }} />
                    ) : (
                        <ArrowsAltOutlined style={{ fontSize: '150%' }} />
                    )}
                </StyledExpandContractButton>
            </Panel>
        </>
    );
};

export default LineageControls;
