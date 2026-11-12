import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';

import { EntityContext } from '@app/entityV2/shared/EntityContext';
import { SidebarSiblingsSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSiblingsSection';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { GetSiblingGroupsDocument } from '@graphql/siblings.generated';
import { EntityType } from '@types';

// localStorage polyfill required before checkAuthStatus runs at module-load time
vi.hoisted(() => {
    const storage = new Map<string, string>();
    Object.defineProperty(globalThis, 'localStorage', {
        configurable: true,
        value: {
            getItem: (key: string) => storage.get(key) ?? null,
            setItem: (key: string, value: string) => storage.set(key, String(value)),
            removeItem: (key: string) => storage.delete(key),
            clear: () => storage.clear(),
            key: (index: number) => Array.from(storage.keys())[index] ?? null,
            get length() {
                return storage.size;
            },
        },
    });
});

const mockShowSeparateSiblings = vi.fn(() => false);
const modalPropsMock = vi.fn();

vi.mock('@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal', () => ({
    EmbeddedListSearchModal: (props: unknown) => {
        modalPropsMock(props);
        return null;
    },
}));

vi.mock('@src/app/useAppConfig', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@src/app/useAppConfig')>();
    return {
        ...actual,
        useIsShowSeparateSiblingsEnabled: () => mockShowSeparateSiblings(),
    };
});

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:iceberg,test.table,PROD)';
const SIBLING_URN = 'urn:li:dataset:(urn:li:dataPlatform:Feature%20Platform,featureset.test,PROD)';

const siblingEntity = {
    urn: SIBLING_URN,
    type: EntityType.Dataset,
    exists: true,
    name: 'featureset.test',
    platform: { name: 'Feature Platform' },
};

const entityDataWithSiblings = {
    urn: DATASET_URN,
    type: EntityType.Dataset,
    exists: true,
    siblingsSearch: {
        total: 1,
        count: 1,
        searchResults: [{ entity: siblingEntity }],
    },
};

const entityDataWithoutSiblings = {
    urn: DATASET_URN,
    type: EntityType.Dataset,
    exists: true,
    siblingsSearch: {
        total: 0,
        count: 0,
        searchResults: [],
    },
};

const dataNotCombinedWithSiblings = {
    dataset: {
        urn: DATASET_URN,
        type: EntityType.Dataset,
        exists: true,
        siblings: null,
        siblingsSearch: null,
        siblingPlatforms: null,
    },
};

function renderSection(entityData: unknown, mocks: MockedResponse[] = []) {
    return render(
        <MockedProvider mocks={mocks} addTypename={false}>
            <TestPageContainer initialEntries={[`/dataset/${DATASET_URN}`]}>
                <EntityContext.Provider
                    value={{
                        urn: DATASET_URN,
                        entityType: EntityType.Dataset,
                        // @ts-expect-error entityData is loose-typed across sidebar sections
                        entityData,
                        baseEntity: { dataset: entityData },
                        routeToTab: vi.fn(),
                        refetch: vi.fn(),
                        lineage: undefined,
                        loading: false,
                        dataNotCombinedWithSiblings,
                    }}
                >
                    <SidebarSiblingsSection />
                </EntityContext.Provider>
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('SidebarSiblingsSection', () => {
    beforeEach(() => {
        mockShowSeparateSiblings.mockReturnValue(false);
    });

    it('renders "Composed of" when siblings exist and showSeparateSiblings is false', () => {
        mockShowSeparateSiblings.mockReturnValue(false);
        const { getByText } = renderSection(entityDataWithSiblings);
        expect(getByText('Composed of')).toBeInTheDocument();
    });

    it('renders "Composed of" when siblings exist and showSeparateSiblings is true', async () => {
        // Regression test: when SHOW_SEPARATE_SIBLINGS=true, the getDataset query previously
        // skipped siblingsSearch entirely via @skip(if: $skipSiblingsSearch), causing this
        // section to never mount. The fix removes @skip from getDataset so siblingsSearch
        // is always fetched on the entity profile page. (With no siblingGroups mock, the group
        // request fails and the section shows the siblingsSearch list once it settles.)
        mockShowSeparateSiblings.mockReturnValue(true);
        renderSection(entityDataWithSiblings);
        expect(await screen.findByText('Composed of')).toBeInTheDocument();
    });

    it('renders nothing when there are no siblings', () => {
        const { container } = renderSection(entityDataWithoutSiblings);
        expect(container.firstChild).toBeNull();
    });

    it('renders nothing when entity data is absent', () => {
        const { container } = renderSection(null);
        expect(container.firstChild).toBeNull();
    });

    it('applies the search bar view to "View all"', () => {
        const { getByText } = renderSection({
            ...entityDataWithSiblings,
            siblingsSearch: { ...entityDataWithSiblings.siblingsSearch, total: 4 },
        });
        fireEvent.click(getByText(/2 more/));

        expect(modalPropsMock).toHaveBeenLastCalledWith(expect.objectContaining({ applyView: true }));
    });

    describe('with siblings as separate results', () => {
        const member = (name: string, isPrimary = false) => ({
            isPrimary,
            entity: {
                urn: `urn:li:dataset:(urn:li:dataPlatform:glue,db.${name},PROD)`,
                type: EntityType.Dataset,
                name,
                platform: null,
                properties: { name },
                container: null,
            },
        });
        const groupMock = (members: unknown[], total: number, truncated = false): MockedResponse => ({
            request: {
                query: GetSiblingGroupsDocument,
                variables: { input: { urns: [DATASET_URN], start: 0, count: 5 } },
            },
            result: { data: { siblingGroups: [{ urn: DATASET_URN, total, truncated, members }] } },
        });
        const fiveMembers = [member('orders_sl', true), ...['sr', 'st', 'tt', 'mx'].map((x) => member(`orders_${x}`))];

        beforeEach(() => {
            mockShowSeparateSiblings.mockReturnValue(true);
        });

        it('shows the group size, the first five and "+ N more" opening search with siblings on', async () => {
            renderSection({ ...entityDataWithSiblings, name: 'orders_pp' }, [groupMock(fiveMembers, 120)]);

            const more = await screen.findByTestId('siblings-more');
            expect(more).toHaveTextContent('+ 115 more');
            expect(more).toHaveAccessibleName('115 more siblings of orders_pp, in search');
            const target = new URL(more.getAttribute('href') || '', 'http://localhost');
            expect(target.pathname).toBe('/search');
            expect(target.searchParams.get('showSiblings')).toBe('true');
            expect(decodeURIComponent(target.searchParams.get('query') || '')).toBe('orders_pp');
            expect(screen.getByText('120')).toBeInTheDocument();
            fiveMembers.forEach(({ entity }) =>
                expect(screen.getByTestId(`compact-entity-link-${entity.urn}`)).toBeInTheDocument(),
            );
        });

        it('shows nothing while the group loads, rather than the one-link list first', () => {
            const { container } = renderSection(entityDataWithSiblings, [groupMock(fiveMembers, 120)]);
            expect(container.firstChild).toBeNull();
        });

        it('marks a group over the size cap as a lower bound', async () => {
            renderSection(entityDataWithSiblings, [groupMock(fiveMembers, 1000, true)]);
            expect(await screen.findByTestId('siblings-more')).toHaveTextContent('+ 995+ more');
            expect(screen.getByText('1000+')).toBeInTheDocument();
        });

        it('has no "+ more" when the whole group is shown', async () => {
            renderSection(entityDataWithSiblings, [groupMock(fiveMembers.slice(0, 2), 2)]);
            await screen.findByTestId(`compact-entity-link-${fiveMembers[0].entity.urn}`);
            expect(screen.queryByTestId('siblings-more')).toBeNull();
        });

        it('shows nothing for an empty group', async () => {
            const { container } = renderSection(entityDataWithSiblings, [groupMock([], 0)]);
            await waitFor(() => expect(container.firstChild).toBeNull());
        });

        it("never shows another dataset's group, and waits instead of showing the fallback list", async () => {
            // Moving between datasets, the previous dataset's data can be returned with loading
            // already false. The section must neither show that group nor swap in the one-link list.
            const otherUrn = 'urn:li:dataset:(urn:li:dataPlatform:glue,db.other,PROD)';
            const { container } = renderSection(entityDataWithSiblings, [
                {
                    ...groupMock(fiveMembers, 120),
                    result: {
                        data: {
                            siblingGroups: [{ urn: otherUrn, total: 120, truncated: false, members: fiveMembers }],
                        },
                    },
                },
            ]);
            // Let the mocked response arrive.
            await act(async () => {
                await new Promise((resolve) => {
                    setTimeout(resolve, 50);
                });
            });
            expect(container.firstChild).toBeNull();
            expect(screen.queryByTestId(`compact-entity-link-${fiveMembers[0].entity.urn}`)).toBeNull();
            expect(screen.queryByTestId(`compact-entity-link-${SIBLING_URN}`)).toBeNull();
        });

        it('falls back to the one-link list when the answer has no group at all', async () => {
            renderSection(entityDataWithSiblings, [
                { ...groupMock(fiveMembers, 120), result: { data: { siblingGroups: [] } } },
            ]);
            await waitFor(() => expect(screen.getByText('Composed of')).toBeInTheDocument());
            expect(screen.getByTestId(`compact-entity-link-${SIBLING_URN}`)).toBeInTheDocument();
        });

        it('falls back to the one-link siblings list when the group request fails', async () => {
            const { container } = renderSection(entityDataWithSiblings, [
                { ...groupMock([], 0), error: new Error('boom') },
            ]);
            // Nothing renders while the request is in flight, so this only passes once it has failed.
            expect(container.firstChild).toBeNull();
            await waitFor(() => expect(screen.getByText('Composed of')).toBeInTheDocument());
            expect(screen.getByTestId(`compact-entity-link-${SIBLING_URN}`)).toBeInTheDocument();
            expect(screen.queryByTestId('siblings-more')).toBeNull();
        });
    });
});
