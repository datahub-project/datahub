import { CheckCircle } from '@phosphor-icons/react/dist/csr/CheckCircle';
import { WarningCircle } from '@phosphor-icons/react/dist/csr/WarningCircle';
import { XCircle } from '@phosphor-icons/react/dist/csr/XCircle';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from 'styled-components';

import { HelperText, ResultContainer } from '@app/context/import/components/importDocumentsModal.styles';
import { Text } from '@src/alchemy-components';

type ImportDocumentsResultStepProps = {
    importError: string | null;
    totalImported: number;
    totalUpdated: number;
    totalFailed: number;
};

const RESULT_ICON_SIZE = 36;

export default function ImportDocumentsResultStep({
    importError,
    totalImported,
    totalUpdated,
    totalFailed,
}: ImportDocumentsResultStepProps) {
    const { t } = useTranslation('misc');
    const theme = useTheme();

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
                <XCircle size={RESULT_ICON_SIZE} color={theme.colors.iconError} weight="fill" />
                <Text weight="semiBold" size="lg">
                    {t('context.import.failedTitle')}
                </Text>
                <HelperText>{importError}</HelperText>
            </ResultContainer>
        );
    }

    return (
        <ResultContainer>
            {totalFailed === 0 ? (
                <CheckCircle size={RESULT_ICON_SIZE} color={theme.colors.iconSuccess} weight="fill" />
            ) : (
                <WarningCircle size={RESULT_ICON_SIZE} color={theme.colors.iconWarning} weight="fill" />
            )}
            <Text weight="semiBold" size="lg">
                {t('context.import.completeTitle')}
            </Text>
            <HelperText>{summary}</HelperText>
        </ResultContainer>
    );
}
