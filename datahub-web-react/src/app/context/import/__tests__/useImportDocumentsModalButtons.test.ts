import { renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import '@app/context/import/__tests__/testSetup';
import { ImportSourceType } from '@app/context/import/import.types';
import { useImportDocumentsModalButtons } from '@app/context/import/useImportDocumentsModalButtons';

describe('useImportDocumentsModalButtons', () => {
    const handlers = {
        onClose: vi.fn(),
        onBack: vi.fn(),
        onImportFiles: vi.fn(),
    };

    it('returns cancel on source step', () => {
        const { result } = renderHook(() =>
            useImportDocumentsModalButtons({
                step: 'source',
                sourceType: null,
                canImport: false,
                ...handlers,
            }),
        );

        expect(result.current).toEqual([{ text: 'Cancel', variant: 'outline', onClick: handlers.onClose }]);
    });

    it('returns back and import on configure step', () => {
        const { result } = renderHook(() =>
            useImportDocumentsModalButtons({
                step: 'configure',
                sourceType: ImportSourceType.GITHUB,
                canImport: true,
                ...handlers,
            }),
        );

        expect(result.current).toEqual([
            { text: 'Back', variant: 'outline', onClick: handlers.onBack },
            { text: 'Import', variant: 'filled', onClick: handlers.onImportFiles, disabled: false },
        ]);
    });

    it('disables import when configure step cannot import', () => {
        const { result } = renderHook(() =>
            useImportDocumentsModalButtons({
                step: 'configure',
                sourceType: ImportSourceType.NOTION,
                canImport: false,
                ...handlers,
            }),
        );

        expect(result.current[1]).toMatchObject({ disabled: true });
    });

    it('returns no buttons while importing', () => {
        const { result } = renderHook(() =>
            useImportDocumentsModalButtons({
                step: 'importing',
                sourceType: ImportSourceType.CONFLUENCE,
                canImport: false,
                ...handlers,
            }),
        );

        expect(result.current).toEqual([]);
    });

    it('returns done on result step', () => {
        const { result } = renderHook(() =>
            useImportDocumentsModalButtons({
                step: 'result',
                sourceType: ImportSourceType.GITHUB,
                canImport: false,
                ...handlers,
            }),
        );

        expect(result.current).toEqual([{ text: 'Done', variant: 'filled', onClick: handlers.onClose }]);
    });
});
