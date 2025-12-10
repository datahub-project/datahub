import React from 'react';

import { NotificationGlobalStyle } from '@components/components/Notification/components/NotificationGlobalStyle';

import { SelectCronGlobalStyles } from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/SelectCronGlobalStyles';

export function GlobalStyles() {
    return (
        <>
            <NotificationGlobalStyle />
            <SelectCronGlobalStyles />
        </>
    );
}
