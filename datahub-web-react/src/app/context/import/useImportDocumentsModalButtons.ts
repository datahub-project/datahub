import { useMemo } from 'react';

import type { ModalButton } from '@components/components/Modal/Modal';

import { ImportSourceType, ImportStep } from '@app/context/import/import.types';

type UseImportDocumentsModalButtonsParams = {
    step: ImportStep;
    sourceType: ImportSourceType | null;
    canImport: boolean;
    onClose: () => void;
    onBack: () => void;
    onImportFiles: () => void;
    onContinueToIngestionSetup: () => void;
};

export function useImportDocumentsModalButtons({
    step,
    sourceType,
    canImport,
    onClose,
    onBack,
    onImportFiles,
    onContinueToIngestionSetup,
}: UseImportDocumentsModalButtonsParams): ModalButton[] {
    return useMemo(() => {
        if (step === 'source') {
            return [{ text: 'Cancel', variant: 'outline', onClick: onClose }];
        }
        if (step === 'configure') {
            if (sourceType && sourceType !== ImportSourceType.FILE_UPLOAD) {
                return [
                    { text: 'Back', variant: 'outline', onClick: onBack },
                    { text: 'Continue to setup', variant: 'filled', onClick: onContinueToIngestionSetup },
                ];
            }
            return [
                { text: 'Back', variant: 'outline', onClick: onBack },
                { text: 'Import', variant: 'filled', onClick: onImportFiles, disabled: !canImport },
            ];
        }
        if (step === 'importing') {
            return [];
        }
        return [{ text: 'Done', variant: 'filled', onClick: onClose }];
    }, [step, sourceType, canImport, onClose, onBack, onImportFiles, onContinueToIngestionSetup]);
}
