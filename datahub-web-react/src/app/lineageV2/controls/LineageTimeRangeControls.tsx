import { ControlPanel, ControlPanelSubtext, ControlPanelTitle } from '@app/lineageV2/controls/common';
import React from 'react';
import LineageTabTimeSelector from '../../entityV2/shared/tabs/Lineage/LineageTabTimeSelector';

const LineageTimeRangeControls = () => {
    return (
        <ControlPanel>
            <ControlPanelTitle>Time Range</ControlPanelTitle>
            <ControlPanelSubtext>Show lineage observed between start and end dates.</ControlPanelSubtext>
            <LineageTabTimeSelector />
        </ControlPanel>
    );
};

export default LineageTimeRangeControls;
