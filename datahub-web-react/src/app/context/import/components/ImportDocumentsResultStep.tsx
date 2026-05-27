import React from 'react';

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
    if (importError) {
        return (
            <ResultContainer>
                <ErrorIcon />
                <Text weight="semiBold" size="lg">
                    Import Failed
                </Text>
                <HelperText>{importError}</HelperText>
            </ResultContainer>
        );
    }

    return (
        <ResultContainer>
            {totalFailed === 0 ? <SuccessIcon /> : <WarningIcon />}
            <Text weight="semiBold" size="lg">
                Import Complete
            </Text>
            <HelperText>
                {totalImported > 0 && `${totalImported} document${totalImported !== 1 ? 's' : ''} created`}
                {totalImported > 0 && totalUpdated > 0 && ', '}
                {totalUpdated > 0 && `${totalUpdated} document${totalUpdated !== 1 ? 's' : ''} updated`}
                {totalFailed > 0 && `, ${totalFailed} document${totalFailed !== 1 ? 's' : ''} failed`}
                {totalImported === 0 && totalUpdated === 0 && totalFailed === 0 && 'No documents imported'}
            </HelperText>
        </ResultContainer>
    );
}
