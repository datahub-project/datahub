import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import {
    ErrorIcon,
    HelperText,
    ResultContainer,
    SuccessIcon,
    WarningIcon,
} from '@app/context/import/components/importDocumentsModal.styles';
import { Text } from '@src/alchemy-components';

type ImportDocumentsResultStepProps = {
    importError: string | null;
    totalImported: number;
    totalUpdated: number;
    totalFailed: number;
};

export default function ImportDocumentsResultStep({
    importError,
    totalImported,
    totalUpdated,
    totalFailed,
}: ImportDocumentsResultStepProps) {
    const { t } = useTranslation('misc');

    const summary = useMemo(() => {
        const parts: string[] = [];
        if (totalImported > 0) {
            parts.push(t('context.import.summaryCreated', { count: totalImported }));
        }
        if (totalUpdated > 0) {
            parts.push(t('context.import.summaryUpdated', { count: totalUpdated }));
        }
        if (totalFailed > 0) {
            parts.push(t('context.import.summaryFailed', { count: totalFailed }));
        }
        return parts.length > 0 ? parts.join(', ') : t('context.import.summaryEmpty');
    }, [t, totalFailed, totalImported, totalUpdated]);

    if (importError) {
        return (
            <ResultContainer>
                <ErrorIcon />
                <Text weight="semiBold" size="lg">
                    {t('context.import.failedTitle')}
                </Text>
                <HelperText>{importError}</HelperText>
            </ResultContainer>
        );
    }

    return (
        <ResultContainer>
            {totalFailed === 0 ? <SuccessIcon /> : <WarningIcon />}
            <Text weight="semiBold" size="lg">
                {t('context.import.completeTitle')}
            </Text>
            <HelperText>{summary}</HelperText>
        </ResultContainer>
    );
}
