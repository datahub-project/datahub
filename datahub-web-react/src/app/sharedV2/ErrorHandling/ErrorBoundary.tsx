/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { ReactNode } from 'react';
import { FallbackProps, ErrorBoundary as ReactErrorBoundary } from 'react-error-boundary';

import ErrorFallback, { ErrorVariant } from '@app/sharedV2/ErrorHandling/ErrorFallback';

type ErrorBoundaryProps = {
    variant?: ErrorVariant;
    children: ReactNode;
    fallback?: React.ComponentType<FallbackProps>;
    resetKeys?: string[];
};

const logError = (error: Error, info: { componentStack: string }) => {
    console.group('🔴 UI Crash Error Report');
    console.error('Error:', error);
    console.error('Component Info:', info);
    console.error('URL:', window.location.href);

    console.warn('🔧 ACTION REQUIRED: Please report this error to your Datahub Administrator');
    console.warn('📧 Include the above error details in your report');
    console.groupEnd();
};

export const ErrorBoundary = ({ children, variant = 'route', fallback, resetKeys }: ErrorBoundaryProps) => {
    const FallbackComponent =
        fallback ||
        (() => (
            <ErrorFallback
                variant={variant}
                // Custom message for on-prem customers
                actionMessage="Please report the error messages from your browser to your Datahub Administrator"
            />
        ));

    return (
        <ReactErrorBoundary
            FallbackComponent={FallbackComponent}
            onError={(e, i) => logError(e, i)}
            resetKeys={resetKeys}
        >
            {children}
        </ReactErrorBoundary>
    );
};
