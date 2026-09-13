import { describe, expect, it } from 'vitest';

import {
    createPendingResourceDocument,
    mergeResourcesDocuments,
    pruneAddedDocuments,
    upsertAddedDocuments,
} from '@app/entityV2/shared/tabs/Documentation/components/useResourcesDocuments.utils';

import { Document, EntityType } from '@types';

const document = (urn: string, title = urn): Document =>
    ({
        urn,
        type: EntityType.Document,
        info: { title },
    }) as Document;

describe('mergeResourcesDocuments', () => {
    it('prepends added documents that the query has not returned yet', () => {
        const merged = mergeResourcesDocuments({
            fetched: [document('fetched')],
            added: [document('added')],
            removedUrns: new Set(),
        });
        expect(merged.map((item) => item.urn)).toEqual(['added', 'fetched']);
    });

    it('filters removed URNs from both fetched and added lists', () => {
        const merged = mergeResourcesDocuments({
            fetched: [document('keep'), document('gone')],
            added: [document('also-gone')],
            removedUrns: new Set(['gone', 'also-gone']),
        });
        expect(merged.map((item) => item.urn)).toEqual(['keep']);
    });

    it('does not duplicate an added document once the query includes it', () => {
        const merged = mergeResourcesDocuments({
            fetched: [document('shared', 'from-query')],
            added: [document('shared', 'from-local')],
            removedUrns: new Set(),
        });
        expect(merged).toHaveLength(1);
        expect(merged[0]?.info?.title).toBe('from-query');
    });
});

describe('pruneAddedDocuments', () => {
    it('drops local rows once they appear in fetched URNs', () => {
        expect(pruneAddedDocuments([document('a'), document('b')], new Set(['a'])).map((item) => item.urn)).toEqual([
            'b',
        ]);
    });
});

describe('upsertAddedDocuments', () => {
    it('replaces an existing local document with the same URN', () => {
        const next = upsertAddedDocuments([document('a', 'old')], [document('a', 'new'), document('b')]);
        expect(next.map((item) => item.urn)).toEqual(['a', 'b']);
        expect(next.find((item) => item.urn === 'a')?.info?.title).toBe('new');
    });
});

describe('createPendingResourceDocument', () => {
    it('uses the create-document title default and records the linked entity for unlink', () => {
        const pending = createPendingResourceDocument('urn:li:document:1', 'urn:li:dataset:asset');
        expect(pending.urn).toBe('urn:li:document:1');
        expect(pending.info?.title).toBe('New Document');
        expect(pending.info?.relatedAssets?.[0]?.asset?.urn).toBe('urn:li:dataset:asset');
    });
});
