import { describe, expect, it, vi } from 'vitest';

import { doDocumentsMatchExpectations, reconcileRelatedDocuments } from '@app/document/hooks/useRelatedDocuments.utils';

import { Document } from '@types';

const document = (urn: string) => ({ urn }) as Document;

describe('useRelatedDocuments utilities', () => {
    it('matches documents that satisfy present and absent expectations', () => {
        expect(
            doDocumentsMatchExpectations([document('present')], {
                presentUrns: ['present'],
                absentUrns: ['removed'],
            }),
        ).toBe(true);
        expect(doDocumentsMatchExpectations([document('removed')], { absentUrns: ['removed'] })).toBe(false);
    });

    it('retries until the expected index state is returned', async () => {
        const fetchDocuments = vi
            .fn<() => Promise<Document[]>>()
            .mockResolvedValueOnce([document('removed')])
            .mockResolvedValueOnce([document('added')]);

        const matched = await reconcileRelatedDocuments({
            fetchDocuments,
            expectations: { presentUrns: ['added'], absentUrns: ['removed'] },
            delaysMs: [0, 0],
            signal: new AbortController().signal,
        });

        expect(matched).toBe(true);
        expect(fetchDocuments).toHaveBeenCalledTimes(2);
    });

    it('does not fetch after cancellation', async () => {
        const controller = new AbortController();
        controller.abort();
        const fetchDocuments = vi.fn<() => Promise<Document[]>>();

        const matched = await reconcileRelatedDocuments({
            fetchDocuments,
            expectations: { presentUrns: ['added'] },
            delaysMs: [0],
            signal: controller.signal,
        });

        expect(matched).toBe(false);
        expect(fetchDocuments).not.toHaveBeenCalled();
    });
});
