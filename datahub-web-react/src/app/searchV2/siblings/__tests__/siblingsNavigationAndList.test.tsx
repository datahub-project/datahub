import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';

import { SearchResultList } from '@app/searchV2/SearchResultList';
import SearchResultSiblings, { EXPANDED_PAGE_SIZE } from '@app/searchV2/siblings/SearchResultSiblings';
import { SiblingGroupData } from '@app/searchV2/siblings/useSiblingGroups';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { GetSiblingGroupsDocument } from '@graphql/siblings.generated';
import { EntityType } from '@types';

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        renderSearchResult: (_type: EntityType, item: any) => <div data-testid="result">{item.entity.urn}</div>,
        getDisplayName: (_type: EntityType, entity: any) => entity.name || entity.urn,
        getEntityUrl: (_type: EntityType, urn: string) => `/dataset/${encodeURIComponent(urn)}`,
        getIcon: () => null,
    }),
}));

const RESULT_URN = 'urn:li:dataset:(urn:li:dataPlatform:glue,db.orders,PROD)';
const CHART_URN = 'urn:li:chart:(looker,c1)';

function member(name: string, isPrimary = false) {
    return {
        __typename: 'SiblingMember',
        isPrimary,
        entity: {
            __typename: 'Dataset',
            urn: `urn:li:dataset:(urn:li:dataPlatform:glue,db.${name},PROD)`,
            type: EntityType.Dataset,
            name,
            platform: null,
            properties: { __typename: 'DatasetProperties', name },
            container: null,
        },
    };
}

const ALL = Array.from({ length: 250 }, (_, i) => member(`copy_${String(i).padStart(3, '0')}`, i === 0));

function group(members: any[], total: number): SiblingGroupData {
    return { __typename: 'SiblingGroup', urn: RESULT_URN, total, truncated: false, members } as SiblingGroupData;
}

function pageMock(start: number, count: number, result: () => any): MockedResponse {
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
        await new Promise((resolve) => {
            setTimeout(resolve, 20);
        });
        expect(first).toHaveBeenCalledTimes(1);
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

    it('leaves out members already merged into the result card', () => {
        render(
            <MockedProvider addTypename>
                <TestPageContainer>
                    <SearchResultSiblings
                        urn={RESULT_URN}
                        resultName="orders"
                        group={group(ALL.slice(0, 5), 7)}
                        excludeUrns={new Set([ALL[1].entity.urn])}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.getAllByTestId('sibling-row')).toHaveLength(4);
        expect(screen.getByText('+ 2 more siblings of orders')).toBeInTheDocument();
    });
});

describe('navigateToSearchUrl and showSiblings', () => {
    function historyAt(search: string) {
        return { location: { search }, push: vi.fn() } as any;
    }

    it('keeps showSiblings when paging or filtering', () => {
        const history = historyAt('?query=orders&showSiblings=true');
        navigateToSearchUrl({ query: 'orders', page: 2, history });
        navigateToSearchUrl({
            query: 'orders',
            filters: [{ field: 'platform', values: ['urn:li:dataPlatform:glue', 'urn:li:dataPlatform:hive'] }],
            history,
        });
        history.push.mock.calls.forEach(([target]) => {
            expect(new URLSearchParams(target.search).get('showSiblings')).toBe('true');
        });
        expect(history.push.mock.calls[1][0].search).toContain('urn%3Ali%3AdataPlatform%3Aglue');
    });

    it('leaves it off when it was off, or when explicitly turned off', () => {
        const off = historyAt('?query=orders');
        navigateToSearchUrl({ query: 'orders', page: 2, history: off });
        expect(off.push.mock.calls[0][0].search).not.toContain('showSiblings');

        const on = historyAt('?query=orders&showSiblings=true');
        navigateToSearchUrl({ query: 'orders', history: on, showSiblings: false });
        expect(on.push.mock.calls[0][0].search).not.toContain('showSiblings');
    });
});

describe('SearchResultList with siblings', () => {
    const results: any[] = [
        { entity: { urn: RESULT_URN, type: EntityType.Dataset, name: 'orders' }, matchedFields: [] },
        { entity: { urn: CHART_URN, type: EntityType.Chart, name: 'chart' }, matchedFields: [] },
    ];

    function renderList(search: string, mocks: MockedResponse[]) {
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
                        highlightedIndex={null}
                        setHighlightedIndex={() => undefined}
                        pageNumber={1}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
    }

    const listMock = (result: () => any): MockedResponse => ({
        request: { query: GetSiblingGroupsDocument, variables: { input: { urns: [RESULT_URN], start: 0, count: 5 } } },
        result,
    });

    it('fetches and renders nothing while the toggle is off', async () => {
        const fetched = vi.fn(() => ({ data: { siblingGroups: [group(ALL.slice(0, 5), 7)] } }));
        renderList('?query=orders', [listMock(fetched)]);
        await new Promise((resolve) => {
            setTimeout(resolve, 20);
        });
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
});
