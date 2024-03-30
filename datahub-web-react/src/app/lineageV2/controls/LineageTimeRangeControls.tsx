import React from 'react';
import { Panel } from 'reactflow';
import styled from 'styled-components';
import LineageTabTimeSelector from '../../entityV2/shared/tabs/Lineage/LineageTabTimeSelector';

const StyledControlsPanel = styled(Panel)<{ isRootPanelExpanded: boolean }>`
    margin-top: 36px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    padding: 16px;
    width: 255px;
    border-radius: 8px;
    border: 1px solid #d5d5d5;
    background: #fff;
    box-shadow: 0px 4px 4px 0px rgba(224, 224, 224, 0.25);
    overflow: hidden;
    margin-left: ${({ isRootPanelExpanded }) => (isRootPanelExpanded ? '178px' : '66px')};
    transition: margin-left 0.3s ease-in-out;
`;

const TimeRangeTitleText = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: #0958d9;
`;

const TimeRangeSubText = styled.div`
    color: #595959;
    font-size: 10px;
    font-weight: 500;
    margin-bottom: 8px;
`;

type Props = {
    isRootPanelExpanded: boolean;
};

const LineageTimeRangeControls = ({ isRootPanelExpanded }: Props) => {
    return (
        <StyledControlsPanel position="top-left" isRootPanelExpanded={isRootPanelExpanded}>
            <TimeRangeTitleText>Time Range</TimeRangeTitleText>
            <TimeRangeSubText>Show edges that were observed after the start date and before the end.</TimeRangeSubText>
            <LineageTabTimeSelector />
        </StyledControlsPanel>
    );
};

export default LineageTimeRangeControls;
