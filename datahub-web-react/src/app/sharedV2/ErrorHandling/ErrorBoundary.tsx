import * as Sentry from '@sentry/react';
import React, { ReactNode } from 'react';
import { FallbackProps, ErrorBoundary as ReactErrorBoundary } from 'react-error-boundary';

import ErrorFallback, { ErrorVariant } from '@app/sharedV2/ErrorHandling/ErrorFallback';
import { useIsOnPremServer } from '@app/useIsOnPremServer';

type ErrorBoundaryProps = {
    variant?: ErrorVariant;
    children: ReactNode;
    fallback?: React.ComponentType<FallbackProps>;
    resetKeys?: string[];
};

const logError = (error: Error, info: { componentStack: string }, enableSentry) => {
    if (enableSentry) {
        // The sentry client is configured in App.tsx
        Sentry.withScope((scope) => {
            scope.setTag('page_url', window.location.href);
            Sentry.captureReactException(error, info);
        });
    } else {
        console.group('🔴 UI Crash Error Report');
        console.error('Error:', error);
        console.error('Component Info:', info);
        console.error('URL:', window.location.href);

        console.warn('🔧 ACTION REQUIRED: Please report this error to your Datahub Administrator');
        console.warn('📧 Include the above error details in your report');
        console.groupEnd();
    }
};

export const ErrorBoundary = ({ children, variant = 'route', fallback, resetKeys }: ErrorBoundaryProps) => {
    // Enable sentry logging only when it's not an on-prem server
    const enableSentry = !useIsOnPremServer();

    const FallbackComponent =
        fallback ||
        (() => (
            <ErrorFallback
                variant={variant}
                // Custom message for on-prem customers
                actionMessage={
                    !enableSentry
                        ? 'Please report the error messages from your browser to your Datahub Administrator'
                        : undefined
                }
            />
        ));

    return (
        <ReactErrorBoundary
            FallbackComponent={FallbackComponent}
            onError={(e, i) => logError(e, i, enableSentry)}
            resetKeys={resetKeys}
        >
            {children}
        </ReactErrorBoundary>
    );
};
