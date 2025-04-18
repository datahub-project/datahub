import React from 'react';

import { FormAnalyticsProvider } from './FormAnalyticsContext';
import { TabLayout } from './TabLayout';

export const GovernDashboard = () => (
    <FormAnalyticsProvider>
        <TabLayout />
    </FormAnalyticsProvider>
);
