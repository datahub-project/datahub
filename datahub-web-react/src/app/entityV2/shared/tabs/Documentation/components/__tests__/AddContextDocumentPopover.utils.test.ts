import { describe, expect, it } from 'vitest';

import {
    getDocumentLinkChanges,
    getSuccessfulDocumentLinkChanges,
} from '@app/entityV2/shared/tabs/Documentation/components/AddContextDocumentPopover.utils';

describe('AddContextDocumentPopover utilities', () => {
    it('finds added and removed documents', () => {
        expect(getDocumentLinkChanges(new Set(['kept', 'removed']), new Set(['kept', 'added']))).toEqual({
            addedUrns: ['added'],
            removedUrns: ['removed'],
        });
    });

    it('maps mutation results back to successful additions and removals', () => {
        expect(
            getSuccessfulDocumentLinkChanges(
                {
                    addedUrns: ['added-success', 'added-failure'],
                    removedUrns: ['removed-success', 'removed-failure'],
                },
                [true, false, true, false],
            ),
        ).toEqual({
            addedUrns: ['added-success'],
            removedUrns: ['removed-success'],
        });
    });
});
