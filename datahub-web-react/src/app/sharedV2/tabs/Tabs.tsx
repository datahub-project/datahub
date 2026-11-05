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

export function Tabs(props: Omit<Props, 'tabErrorBoundary'>) {
    return <AlchemyTabs {...props} tabErrorBoundary={TabErrorBoundary} />;
}
