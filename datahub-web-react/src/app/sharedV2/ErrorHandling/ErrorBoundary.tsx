import React, { ReactNode } from 'react';
import { FallbackProps, ErrorBoundary as ReactErrorBoundary } from 'react-error-boundary';
import { useTranslation } from 'react-i18next';

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

    console.warn('🔧 ACTION REQUIRED: Please report this error to your DataHub Administrator');
    console.warn('📧 Include the above error details in your report');
    console.groupEnd();
};

export const ErrorBoundary = ({ children, variant = 'route', fallback, resetKeys }: ErrorBoundaryProps) => {
    const { t } = useTranslation('shared.error');
    const FallbackComponent =
        fallback ||
        (() => (
            <ErrorFallback
                variant={variant}
                // Custom message for on-prem customers
                actionMessage={t('boundary.onPremActionMessage')}
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
