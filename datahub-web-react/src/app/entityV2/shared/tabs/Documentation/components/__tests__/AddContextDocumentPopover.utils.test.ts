import { describe, expect, it } from 'vitest';

import {
    getDocumentLinkChanges,
    getSuccessfulDocumentLinkChanges,
} from '@app/entityV2/shared/tabs/Documentation/components/AddContextDocumentPopover.utils';

import { Document, EntityType } from '@types';

const document = (urn: string): Document => ({ urn, type: EntityType.Document }) as Document;

describe('AddContextDocumentPopover utilities', () => {
    it('finds added and removed documents', () => {
        expect(getDocumentLinkChanges(new Set(['kept', 'removed']), new Set(['kept', 'added']))).toEqual({
            addedUrns: ['added'],
            removedUrns: ['removed'],
        });
    });

    it('keeps only documents and removals whose mutations succeeded', () => {
        expect(
            getSuccessfulDocumentLinkChanges(
                [
                    { ok: true, document: document('added-success') },
                    { ok: false, document: document('added-failure') },
                ],
                ['removed-success', 'removed-failure'],
                [
                    { ok: true, document: null },
                    { ok: false, document: null },
                ],
            ),
        ).toEqual({
            addedDocuments: [document('added-success')],
            removedUrns: ['removed-success'],
        });
    });
});
