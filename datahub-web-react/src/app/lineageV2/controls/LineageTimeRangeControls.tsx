import React from 'react';

import LineageTabTimeSelector from '@app/entityV2/shared/tabs/Lineage/LineageTabTimeSelector';
import { ControlPanel, ControlPanelSubtext, ControlPanelTitle } from '@app/lineageV2/controls/common';

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
