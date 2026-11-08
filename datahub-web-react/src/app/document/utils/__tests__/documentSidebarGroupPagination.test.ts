import {
    advanceDocumentGroupPagination,
    createDocumentGroupPaginationState,
    getDocumentGroupPaginationView,
    mergeDocumentGroupPaginationPage,
    shouldAdvanceDocumentGroupPagination,
} from '@app/document/utils/documentSidebarGroupPagination';

type TestDoc = {
    urn: string;
    title: string;
};

describe('document sidebar group pagination', () => {
    it('replaces accumulated rows with the first page', () => {
        const initial = createDocumentGroupPaginationState<TestDoc>('domain-a');
        const fresh = [{ urn: 'a', title: 'A' }];

        expect(mergeDocumentGroupPaginationPage(initial, 'domain-a', 0, fresh, 1)).toEqual({
            criteriaKey: 'domain-a',
            start: 0,
            documents: fresh,
            total: 1,
        });
    });

    it('appends only when pageStart matches the current length', () => {
        const firstPage = mergeDocumentGroupPaginationPage(
            createDocumentGroupPaginationState<TestDoc>('domain-a'),
            'domain-a',
            0,
            [{ urn: 'a', title: 'A' }],
            3,
        );
        const second = [{ urn: 'b', title: 'B' }];

        expect(mergeDocumentGroupPaginationPage(firstPage, 'domain-a', 1, second, 3).documents).toEqual([
            { urn: 'a', title: 'A' },
            { urn: 'b', title: 'B' },
        ]);
        // Out-of-order / wrong offset is ignored.
        expect(mergeDocumentGroupPaginationPage(firstPage, 'domain-a', 2, second, 3)).toBe(firstPage);
    });

    it('ignores non-zero pages for a stale criteria key', () => {
        const oldState = mergeDocumentGroupPaginationPage(
            createDocumentGroupPaginationState<TestDoc>('old'),
            'old',
            0,
            [{ urn: 'old', title: 'Old' }],
            1,
        );

        expect(mergeDocumentGroupPaginationPage(oldState, 'new', 1, [{ urn: 'x', title: 'X' }], 2)).toBe(oldState);
        expect(mergeDocumentGroupPaginationPage(oldState, 'new', 0, [{ urn: 'new', title: 'New' }], 1)).toEqual({
            criteriaKey: 'new',
            start: 0,
            documents: [{ urn: 'new', title: 'New' }],
            total: 1,
        });
    });

    it('exposes empty rows as soon as criteria change', () => {
        const paginated = advanceDocumentGroupPagination(
            mergeDocumentGroupPaginationPage(
                createDocumentGroupPaginationState<TestDoc>('a'),
                'a',
                0,
                [{ urn: 'a', title: 'A' }],
                10,
            ),
            'a',
            1,
        );

        expect(getDocumentGroupPaginationView(paginated, 'b')).toEqual({
            start: 0,
            documents: [],
            total: 0,
        });
    });

    it('advances the offset only when more rows are loaded beyond start', () => {
        expect(
            shouldAdvanceDocumentGroupPagination({
                loading: false,
                hasError: false,
                documentsLength: 50,
                total: 100,
                start: 0,
                inView: true,
            }),
        ).toBe(true);

        expect(
            shouldAdvanceDocumentGroupPagination({
                loading: true,
                hasError: false,
                documentsLength: 50,
                total: 100,
                start: 0,
                inView: true,
            }),
        ).toBe(false);

        expect(
            shouldAdvanceDocumentGroupPagination({
                loading: false,
                hasError: false,
                documentsLength: 50,
                total: 100,
                start: 50,
                inView: true,
            }),
        ).toBe(false);

        expect(
            shouldAdvanceDocumentGroupPagination({
                loading: false,
                hasError: false,
                documentsLength: 100,
                total: 100,
                start: 50,
                inView: true,
            }),
        ).toBe(false);
    });
});
