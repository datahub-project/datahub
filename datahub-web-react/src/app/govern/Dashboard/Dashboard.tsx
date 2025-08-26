import React from 'react';

import { FormAnalyticsProvider } from '@app/govern/Dashboard/FormAnalyticsContext';
import { TabLayout } from '@app/govern/Dashboard/TabLayout';

export const GovernDashboard = () => (
    <FormAnalyticsProvider>
        <TabLayout />
    </FormAnalyticsProvider>
);
