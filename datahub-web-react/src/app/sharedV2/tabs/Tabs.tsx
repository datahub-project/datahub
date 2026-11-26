import React from 'react';

import { Tabs as AlchemyTabs, Props, TabErrorBoundaryProps } from '@components/components/Tabs/Tabs';

import { ErrorBoundary } from '@app/sharedV2/ErrorHandling/ErrorBoundary';

function TabErrorBoundary({ resetKeys, children }: TabErrorBoundaryProps) {
    return (
        <ErrorBoundary resetKeys={resetKeys} variant="tab">
            {children}
        </ErrorBoundary>
    );
}

/**
 * The Tabs for app code: alchemy Tabs with each tab's content in the app ErrorBoundary, so a crashing tab
 * shows an error state instead of taking the page down. Import this one, not Tabs from '@components'.
 */
export function Tabs(props: Omit<Props, 'tabErrorBoundary'>) {
    return <AlchemyTabs {...props} tabErrorBoundary={TabErrorBoundary} />;
}
