import React from 'react';
import { useTranslation } from 'react-i18next';

import LineageTabTimeSelector from '@app/entityV2/shared/tabs/Lineage/LineageTabTimeSelector';
import { ControlPanel, ControlPanelSubtext, ControlPanelTitle } from '@app/lineageV2/controls/common';

const LineageTimeRangeControls = () => {
    const { t } = useTranslation('lineage');
    return (
        <ControlPanel>
            <ControlPanelTitle>{t('controls.timeRange.title')}</ControlPanelTitle>
            <ControlPanelSubtext>{t('controls.timeRange.description')}</ControlPanelSubtext>
            <LineageTabTimeSelector />
        </ControlPanel>
    );
};

export default LineageTimeRangeControls;
