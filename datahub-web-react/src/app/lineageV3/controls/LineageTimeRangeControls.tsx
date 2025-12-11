/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import LineageTabTimeSelector from '@app/entityV2/shared/tabs/Lineage/LineageTabTimeSelector';
import { ControlPanel, ControlPanelSubtext, ControlPanelTitle } from '@app/lineageV3/controls/common';

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
