import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

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
    const { t } = useTranslation('misc');

    return useMemo(() => {
        if (step === 'source') {
            return [{ text: t('context.import.cancel'), variant: 'text', color: 'gray', onClick: onClose }];
        }
        if (step === 'configure') {
            return [
                { text: t('context.import.back'), variant: 'text', color: 'primary', onClick: onBack },
                {
                    text: t('context.import.import'),
                    variant: 'filled',
                    onClick: onImportFiles,
                    disabled: !canImport,
                },
            ];
        }
        if (step === 'importing') {
            return [];
        }
        return [{ text: t('context.import.done'), variant: 'filled', onClick: onClose }];
    }, [step, canImport, onClose, onBack, onImportFiles, t]);
}
