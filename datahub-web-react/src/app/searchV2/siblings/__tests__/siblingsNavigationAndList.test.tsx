import { FetchResult } from '@apollo/client';
import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { RouteComponentProps } from 'react-router-dom';

import analytics, { EventType } from '@app/analytics';
import { PLATFORM_URN_TO_LOGO } from '@app/ingestV2/source/builder/constants';
import { SearchResultList } from '@app/searchV2/SearchResultList';
import SearchFilters from '@app/searchV2/filters/SearchFilters';
import SearchResultSiblings, { EXPANDED_PAGE_SIZE } from '@app/searchV2/siblings/SearchResultSiblings';
import { RESULT_URN, group, member, settle } from '@app/searchV2/siblings/__tests__/siblingFixtures';
import { SiblingMemberData } from '@app/searchV2/siblings/useSiblingGroups';
import { CombinedSearchResult } from '@app/searchV2/utils/combineSiblingsInSearchResults';
import { UnionType } from '@app/searchV2/utils/constants';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { GetSiblingGroupsDocument } from '@graphql/siblings.generated';
import { EntityType } from '@types';

let separateSiblings = true;
vi.mock('@app/useAppConfig', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/useAppConfig')>()),
    useIsShowSeparateSiblingsEnabled: () => separateSiblings,
}));

type NamedEntity = { urn: string; name?: string };

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        renderSearchResult: (_type: EntityType, item: { entity: NamedEntity }) => (
            <div data-testid="result">{item.entity.urn}</div>
        ),
        getDisplayName: (_type: EntityType, entity: NamedEntity) => entity.name || entity.urn,
        getEntityUrl: (_type: EntityType, urn: string) => `/dataset/${encodeURIComponent(urn)}`,
        getIcon: () => null,
    }),
}));

const CHART_URN = 'urn:li:chart:(looker,c1)';

const ALL = Array.from({ length: 250 }, (_, i) => member(`copy_${String(i).padStart(3, '0')}`, i === 0));

function pageMock(start: number, count: number, result: () => FetchResult): MockedResponse {
    return {
        request: { query: GetSiblingGroupsDocument, variables: { input: { urns: [RESULT_URN], start, count } } },
        result,
    };
}

function scrollToBottom(list: HTMLElement) {
    Object.defineProperty(list, 'scrollHeight', { value: 1000, configurable: true });
    Object.defineProperty(list, 'clientHeight', { value: 360, configurable: true });
    Object.defineProperty(list, 'scrollTop', { value: 640, configurable: true });
    fireEvent.scroll(list);
}

beforeEach(() => {
    separateSiblings = true;
});

describe('expanded sibling list paging', () => {
    it('requests the next page on scroll and stops after a short page', async () => {
        const first = vi.fn(() => ({ data: { siblingGroups: [group(ALL.slice(0, 200), 250)] } }));
        const second = vi.fn(() => ({ data: { siblingGroups: [group(ALL.slice(200), 250)] } }));
        render(
            <MockedProvider
                mocks={[pageMock(0, EXPANDED_PAGE_SIZE, first), pageMock(200, EXPANDED_PAGE_SIZE, second)]}
                addTypename
            >
                <TestPageContainer>
                    <SearchResultSiblings urn={RESULT_URN} resultName="orders" group={group(ALL.slice(0, 5), 250)} />
                </TestPageContainer>
            </MockedProvider>,
        );
        fireEvent.click(screen.getByText('+ 245 more siblings of orders'));
        await waitFor(() => expect(screen.getAllByTestId('sibling-compact-row')).toHaveLength(200));

        const list = screen.getByRole('list', { name: '250 siblings of orders' });
        scrollToBottom(list);
        await waitFor(() => expect(screen.getAllByTestId('sibling-compact-row')).toHaveLength(250));
        expect(second).toHaveBeenCalledTimes(1);

        // The second page was short, so the list is complete: scrolling again requests nothing.
        scrollToBottom(list);
        await settle();
        expect(first).toHaveBeenCalledTimes(1);
        expect(second).toHaveBeenCalledTimes(1);
    });

    it('skips members that failed to load, and pages from the server offset', async () => {
        // Members whose entity failed to load come back with entity null.
        const unloaded = (m: SiblingMemberData) => ({ ...m, entity: null }) as unknown as SiblingMemberData;
        const firstPage = ALL.slice(0, 200).map((m, i) => (i % 2 ? unloaded(m) : m));
        const first = vi.fn(() => ({ data: { siblingGroups: [group(firstPage, 250)] } }));
        const second = vi.fn(() => ({ data: { siblingGroups: [group(ALL.slice(200), 250)] } }));
        render(
            <MockedProvider
                mocks={[pageMock(0, EXPANDED_PAGE_SIZE, first), pageMock(200, EXPANDED_PAGE_SIZE, second)]}
                addTypename
            >
                <TestPageContainer>
                    <SearchResultSiblings urn={RESULT_URN} resultName="orders" group={group(ALL.slice(0, 5), 250)} />
                </TestPageContainer>
            </MockedProvider>,
        );
        fireEvent.click(screen.getByText('+ 245 more siblings of orders'));
        await waitFor(() => expect(screen.getAllByTestId('sibling-compact-row')).toHaveLength(100));

        // The next request starts after the 200 the server returned, not after the 100 shown.
        scrollToBottom(screen.getByRole('list', { name: '250 siblings of orders' }));
        await waitFor(() => expect(screen.getAllByTestId('sibling-compact-row')).toHaveLength(150));
        expect(second).toHaveBeenCalledTimes(1);
    });

    it('shows an error with Retry when a page fails, and retries', async () => {
        render(
            <MockedProvider
                mocks={[
                    { ...pageMock(0, EXPANDED_PAGE_SIZE, () => ({})), error: new Error('boom') },
                    pageMock(0, EXPANDED_PAGE_SIZE, () => ({ data: { siblingGroups: [group(ALL.slice(0, 7), 7)] } })),
                ]}
                addTypename
            >
                <TestPageContainer>
                    <SearchResultSiblings urn={RESULT_URN} resultName="orders" group={group(ALL.slice(0, 5), 7)} />
                </TestPageContainer>
            </MockedProvider>,
        );
        fireEvent.click(screen.getByText('+ 2 more siblings of orders'));
        await waitFor(() => expect(screen.getByText("Couldn't load siblings.")).toBeInTheDocument());

        fireEvent.click(screen.getByText('Retry'));
        await waitFor(() => expect(screen.getAllByTestId('sibling-compact-row')).toHaveLength(7));
    });

    it('moves focus into the list on expand and back on collapse', async () => {
        render(
            <MockedProvider
                mocks={[
                    pageMock(0, EXPANDED_PAGE_SIZE, () => ({ data: { siblingGroups: [group(ALL.slice(0, 7), 7)] } })),
                ]}
                addTypename
            >
                <TestPageContainer>
                    <SearchResultSiblings urn={RESULT_URN} resultName="orders" group={group(ALL.slice(0, 5), 7)} />
                </TestPageContainer>
            </MockedProvider>,
        );
        fireEvent.click(screen.getByText('+ 2 more siblings of orders'));
        await waitFor(() => expect(screen.getByRole('list', { name: '7 siblings of orders' })).toHaveFocus());
        fireEvent.click(screen.getByText('Show less'));
        expect(screen.getByText('+ 2 more siblings of orders')).toHaveFocus();
    });

    it("shows the platform's bundled logo in compact rows, and the default icon if it fails", async () => {
        const snowflakeUrn = 'urn:li:dataPlatform:snowflake';
        const withPlatform = ALL.slice(0, 7).map((m) => ({
            ...m,
            entity: {
                ...m.entity,
                platform: { __typename: 'DataPlatform', urn: snowflakeUrn, name: 'snowflake', properties: null },
            },
        })) as SiblingMemberData[];
        render(
            <MockedProvider
                mocks={[pageMock(0, EXPANDED_PAGE_SIZE, () => ({ data: { siblingGroups: [group(withPlatform, 7)] } }))]}
                addTypename
            >
                <TestPageContainer>
                    <SearchResultSiblings
                        urn={RESULT_URN}
                        resultName="orders"
                        group={group(withPlatform.slice(0, 5), 7)}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        fireEvent.click(screen.getByText('+ 2 more siblings of orders'));
        await waitFor(() => expect(screen.getAllByTestId('sibling-compact-row')).toHaveLength(7));
        const firstRow = screen.getAllByTestId('sibling-compact-row')[0];
        const logo = firstRow.querySelector('img') as HTMLImageElement;
        expect(logo.getAttribute('src')).toBe(PLATFORM_URN_TO_LOGO[snowflakeUrn]);

        fireEvent.error(logo);
        expect(firstRow.querySelector('img')).toBeNull();
    });

    it('ignores a page that arrives after unmount', async () => {
        const consoleError = vi.spyOn(console, 'error');
        const { unmount } = render(
            <MockedProvider
                mocks={[
                    {
                        ...pageMock(0, EXPANDED_PAGE_SIZE, () => ({
                            data: { siblingGroups: [group(ALL.slice(0, 7), 7)] },
                        })),
                        delay: 30,
                    },
                ]}
                addTypename
            >
                <TestPageContainer>
                    <SearchResultSiblings urn={RESULT_URN} resultName="orders" group={group(ALL.slice(0, 5), 7)} />
                </TestPageContainer>
            </MockedProvider>,
        );
        fireEvent.click(screen.getByText('+ 2 more siblings of orders'));
        unmount();
        await new Promise((resolve) => {
            setTimeout(resolve, 60);
        });
        // The test providers warn about their own updates; only a warning from this component counts.
        const ownWarning = consoleError.mock.calls.some(
            (args) =>
                args.some((arg) => String(arg).includes('unmounted component')) &&
                args.some((arg) => String(arg).includes('SearchResultSiblings')),
        );
        expect(ownWarning).toBe(false);
        consoleError.mockRestore();
    });
});

describe('navigateToSearchUrl and showSiblings', () => {
    type HistoryStub = Pick<RouteComponentProps['history'], 'location' | 'push'> & {
        push: ReturnType<typeof vi.fn>;
    };
    // A stub with only what navigateToSearchUrl uses.
    const asHistory = (stub: HistoryStub) => stub as unknown as RouteComponentProps['history'];
    function historyAt(search: string): HistoryStub {
        return { location: { search } as RouteComponentProps['history']['location'], push: vi.fn() };
    }

    it('keeps showSiblings when paging or filtering', () => {
        const history = historyAt('?query=orders&showSiblings=true');
        navigateToSearchUrl({ query: 'orders', page: 2, history: asHistory(history) });
        navigateToSearchUrl({
            query: 'orders',
            filters: [{ field: 'platform', values: ['urn:li:dataPlatform:glue', 'urn:li:dataPlatform:hive'] }],
            history: asHistory(history),
        });
        history.push.mock.calls.forEach(([target]) => {
            expect(new URLSearchParams(target.search).get('showSiblings')).toBe('true');
        });
        expect(history.push.mock.calls[1][0].search).toContain('urn%3Ali%3AdataPlatform%3Aglue');
    });

    it('leaves it off when it was off, or when navigating from a page without it', () => {
        const off = historyAt('?query=orders');
        navigateToSearchUrl({ query: 'orders', page: 2, history: asHistory(off) });
        expect(off.push.mock.calls[0][0].search).not.toContain('showSiblings');

        const home = historyAt('');
        navigateToSearchUrl({ query: 'orders', history: asHistory(home) });
        expect(home.push.mock.calls[0][0].search).not.toContain('showSiblings');
    });
});

describe('SearchResultList with siblings', () => {
    beforeAll(() => {
        // jsdom has no scrollIntoView; the list scrolls the highlighted result into view.
        Element.prototype.scrollIntoView = vi.fn();
    });

    const results = [
        { entity: { urn: RESULT_URN, type: EntityType.Dataset, name: 'orders' }, matchedFields: [] },
        { entity: { urn: CHART_URN, type: EntityType.Chart, name: 'chart' }, matchedFields: [] },
    ] as unknown as CombinedSearchResult[];

    function renderList(
        search: string,
        mocks: MockedResponse[],
        setHighlightedIndex: (index: number | null) => void = () => undefined,
    ) {
        return render(
            <MockedProvider mocks={mocks} addTypename>
                <TestPageContainer initialEntries={[`/search${search}`]}>
                    <SearchResultList
                        loading={false}
                        query="orders"
                        searchResults={results}
                        totalResultCount={2}
                        isSelectMode={false}
                        selectedEntities={[]}
                        setSelectedEntities={() => undefined}
                        suggestions={[]}
                        highlightedIndex={0}
                        setHighlightedIndex={setHighlightedIndex}
                        pageNumber={1}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
    }

    const listMock = (result: () => FetchResult): MockedResponse => ({
        request: { query: GetSiblingGroupsDocument, variables: { input: { urns: [RESULT_URN], start: 0, count: 5 } } },
        result,
    });

    it('fetches and renders nothing while the toggle is off', async () => {
        const fetched = vi.fn(() => ({ data: { siblingGroups: [group(ALL.slice(0, 5), 7)] } }));
        renderList('?query=orders', [listMock(fetched)]);
        expect(screen.getAllByTestId('result')).toHaveLength(2);
        await settle();
        expect(fetched).not.toHaveBeenCalled();
        expect(screen.queryAllByTestId('sibling-row')).toHaveLength(0);
    });

    it('renders siblings only under dataset results when on', async () => {
        renderList('?query=orders&showSiblings=true', [
            listMock(() => ({ data: { siblingGroups: [group(ALL.slice(0, 5), 7)] } })),
        ]);
        await waitFor(() => expect(screen.getAllByTestId('sibling-row')).toHaveLength(5));
        expect(screen.getByTestId(`siblings-${RESULT_URN}`)).toBeInTheDocument();
        expect(screen.queryByTestId(`siblings-${CHART_URN}`)).toBeNull();
    });

    it('shows an error with Retry when the siblings request fails, and retries', async () => {
        renderList('?query=orders&showSiblings=true', [
            { ...listMock(() => ({})), error: new Error('boom') },
            listMock(() => ({ data: { siblingGroups: [group(ALL.slice(0, 5), 7)] } })),
        ]);
        await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent("Couldn't load siblings."));
        fireEvent.click(screen.getByText('Retry'));
        await waitFor(() => expect(screen.getAllByTestId('sibling-row')).toHaveLength(5));
    });

    it('leaves arrows inside the expanded list alone, but not Escape or keys elsewhere in the siblings', async () => {
        const setHighlightedIndex = vi.fn();
        renderList(
            '?query=orders&showSiblings=true',
            [
                listMock(() => ({ data: { siblingGroups: [group(ALL.slice(0, 5), 7)] } })),
                pageMock(0, EXPANDED_PAGE_SIZE, () => ({ data: { siblingGroups: [group(ALL.slice(0, 7), 7)] } })),
            ],
            setHighlightedIndex,
        );
        await waitFor(() => expect(screen.getAllByTestId('sibling-row')).toHaveLength(5));
        const more = screen.getByText('+ 2 more siblings of orders');
        fireEvent.keyDown(more, { key: 'ArrowDown' });
        expect(setHighlightedIndex).toHaveBeenLastCalledWith(1);
        fireEvent.keyDown(more, { key: 'Escape' });
        expect(setHighlightedIndex).toHaveBeenLastCalledWith(null);

        fireEvent.click(more);
        await waitFor(() => expect(screen.getAllByTestId('sibling-compact-row')).toHaveLength(7));
        setHighlightedIndex.mockClear();
        const list = screen.getByRole('list', { name: '7 siblings of orders' });
        fireEvent.keyDown(list, { key: 'ArrowDown' });
        fireEvent.keyDown(list, { key: 'ArrowUp' });
        expect(setHighlightedIndex).not.toHaveBeenCalled();
        fireEvent.keyDown(list, { key: 'Escape' });
        expect(setHighlightedIndex).toHaveBeenCalledWith(null);
    });

    it('logs a sibling click at the parent result position, marked with siblingOf', async () => {
        const event = vi.spyOn(analytics, 'event').mockResolvedValue(undefined);
        renderList('?query=orders&showSiblings=true', [
            listMock(() => ({ data: { siblingGroups: [group(ALL.slice(0, 5), 7)] } })),
        ]);
        await waitFor(() => expect(screen.getAllByTestId('sibling-row')).toHaveLength(5));
        fireEvent.click(screen.getByText('copy_001'));
        expect(event).toHaveBeenCalledWith({
            type: EventType.SearchResultClickEvent,
            query: 'orders',
            entityUrn: ALL[1].entity.urn,
            entityType: EntityType.Dataset,
            index: 0,
            total: 2,
            pageNumber: 1,
            siblingOf: RESULT_URN,
        });
        event.mockRestore();
    });

    it('shows no toggle and no rows in the combined-siblings view, even with showSiblings=true', async () => {
        separateSiblings = false;
        const fetched = vi.fn(() => ({ data: { siblingGroups: [group(ALL.slice(0, 5), 7)] } }));
        renderList('?query=orders&showSiblings=true', [listMock(fetched)]);
        expect(screen.getAllByTestId('result')).toHaveLength(2);
        await settle();
        expect(fetched).not.toHaveBeenCalled();
        expect(screen.queryAllByTestId('sibling-row')).toHaveLength(0);

        render(
            <MockedProvider addTypename>
                <TestPageContainer initialEntries={['/search?query=orders&showSiblings=true']}>
                    <SearchFilters
                        loading={false}
                        availableFilters={[]}
                        activeFilters={[]}
                        unionType={UnionType.AND}
                        onChangeFilters={() => undefined}
                        onChangeUnionType={() => undefined}
                        onClearFilters={() => undefined}
                        query="orders"
                        downloadSearchResults={async () => null}
                        showSiblingsToggle
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.queryByTestId('show-siblings-toggle')).toBeNull();
    });
});
