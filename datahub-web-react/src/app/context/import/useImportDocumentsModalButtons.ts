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
};

export function useImportDocumentsModalButtons({
    step,
    canImport,
    onClose,
    onBack,
    onImportFiles,
}: UseImportDocumentsModalButtonsParams): ModalButton[] {
    return useMemo(() => {
        if (step === 'source') {
            return [{ text: 'Cancel', variant: 'outline', onClick: onClose }];
        }
        if (step === 'configure') {
            return [
                { text: 'Back', variant: 'outline', onClick: onBack },
                { text: 'Import', variant: 'filled', onClick: onImportFiles, disabled: !canImport },
            ];
        }
        if (step === 'importing') {
            return [];
        }
        return [{ text: 'Done', variant: 'filled', onClick: onClose }];
    }, [step, canImport, onClose, onBack, onImportFiles]);
}
