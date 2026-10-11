import { ArrowsClockwise } from '@phosphor-icons/react/dist/csr/ArrowsClockwise';
import { ExclamationMark } from '@phosphor-icons/react/dist/csr/ExclamationMark';
import React from 'react';
import { FallbackProps } from 'react-error-boundary';
import { useTranslation } from 'react-i18next';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

interface Props {
    fallbackProps: FallbackProps;
    moduleProps: ModuleProps;
}

export default function LargeModuleFallback({ moduleProps, fallbackProps }: Props) {
    const { t } = useTranslation('modules');
    return (
        <LargeModule {...moduleProps}>
            <EmptyContent
                title={t('error.oopsTitle')}
                description={t('error.largeDescription')}
                icon={ExclamationMark}
                linkText={t('error.refreshLink')}
                linkIcon={ArrowsClockwise}
                onLinkClick={() => fallbackProps.resetErrorBoundary()}
            />
        </LargeModule>
    );
}
