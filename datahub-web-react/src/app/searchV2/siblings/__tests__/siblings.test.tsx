import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { useLocation } from 'react-router-dom';

import SearchResultSiblings, { EXPANDED_PAGE_SIZE } from '@app/searchV2/siblings/SearchResultSiblings';
import { RESULT_URN, group as fixtureGroup, member, settle } from '@app/searchV2/siblings/__tests__/siblingFixtures';
import { SHOW_SIBLINGS_URL_PARAM, useShowSiblings } from '@app/searchV2/siblings/useShowSiblings';
import { SiblingMemberData, useSiblingGroups } from '@app/searchV2/siblings/useSiblingGroups';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { GetSiblingGroupsDocument } from '@graphql/siblings.generated';

let separateSiblings = true;
vi.mock('@app/useAppConfig', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/useAppConfig')>()),
    useIsShowSeparateSiblingsEnabled: () => separateSiblings,
}));

const MEMBERS = [member('orders_primary', true), ...Array.from({ length: 6 }, (_, i) => member(`copy_${i}`))];

const group = (members = MEMBERS, total = MEMBERS.length) => fixtureGroup(members, total);

function siblingGroupsMock(start: number, count: number, members = MEMBERS): MockedResponse {
    return {
        request: {
            query: GetSiblingGroupsDocument,
            variables: { input: { urns: [RESULT_URN], start, count } },
        },
        result: { data: { siblingGroups: [group(members.slice(start, start + count))] } },
    };
}

beforeEach(() => {
    separateSiblings = true;
});

describe('useShowSiblings', () => {
    const filterParam = 'filter_platform___false___EQUAL___0';
    const search = `?query=orders&${filterParam}=${encodeURIComponent('urn:li:dataPlatform:glue')},${encodeURIComponent(
        'urn:li:dataPlatform:hive',
    )}`;

    function renderToggle(initial: string) {
        return renderHook(() => ({ toggle: useShowSiblings(), location: useLocation() }), {
            wrapper: ({ children }) => (
                <MockedProvider>
                    <TestPageContainer initialEntries={[`/search${initial}`]}>{children}</TestPageContainer>
                </MockedProvider>
            ),
        });
    }

    it('reads the URL param and changes only that param, keeping multi-value comma filters', () => {
        const { result } = renderToggle(search);

        expect(result.current.toggle[0]).toBe(false);
        act(() => result.current.toggle[1](true));
        expect(result.current.toggle[0]).toBe(true);
        const params = new URLSearchParams(result.current.location.search);
        expect(params.get(SHOW_SIBLINGS_URL_PARAM)).toBe('true');
        expect(params.get('query')).toBe('orders');
        expect(params.get(filterParam)).toBe('urn:li:dataPlatform:glue,urn:li:dataPlatform:hive');

        act(() => result.current.toggle[1](false));
        const after = new URLSearchParams(result.current.location.search);
        expect(after.has(SHOW_SIBLINGS_URL_PARAM)).toBe(false);
        expect(after.get(filterParam)).toBe('urn:li:dataPlatform:glue,urn:li:dataPlatform:hive');
    });

    it('ignores the param in the combined-siblings view', () => {
        separateSiblings = false;
        const { result } = renderToggle(`${search}&${SHOW_SIBLINGS_URL_PARAM}=true`);
        expect(result.current.toggle[0]).toBe(false);
    });
});

describe('useSiblingGroups', () => {
    it('sends no request while the toggle is off', async () => {
        const fetched = vi.fn(() => ({ data: { siblingGroups: [group()] } }));
        const mocks = [{ ...siblingGroupsMock(0, 5), result: fetched }];
        const { result } = renderHook(() => useSiblingGroups([RESULT_URN], false), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename>
                    <TestPageContainer>{children}</TestPageContainer>
                </MockedProvider>
            ),
        });
        await settle();
        expect(fetched).not.toHaveBeenCalled();
        expect(result.current.groups.size).toBe(0);
    });

    it('fetches the first five members of each group when on', async () => {
        const { result, waitForNextUpdate } = renderHook(() => useSiblingGroups([RESULT_URN], true), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={[siblingGroupsMock(0, 5)]} addTypename>
                    <TestPageContainer>{children}</TestPageContainer>
                </MockedProvider>
            ),
        });
        await waitForNextUpdate();
        expect(result.current.groups.get(RESULT_URN)?.members).toHaveLength(5);
    });

    it('skips a member that failed to load', async () => {
        const failed = { ...member('broken'), entity: null } as unknown as SiblingMemberData;
        const { result, waitForNextUpdate } = renderHook(() => useSiblingGroups([RESULT_URN], true), {
            wrapper: ({ children }) => (
                <MockedProvider
                    mocks={[
                        {
                            ...siblingGroupsMock(0, 5),
                            result: { data: { siblingGroups: [group([MEMBERS[0], failed, MEMBERS[1]], 7)] } },
                        },
                    ]}
                    addTypename
                >
                    <TestPageContainer>{children}</TestPageContainer>
                </MockedProvider>
            ),
        });
        await waitForNextUpdate();
        expect(result.current.groups.get(RESULT_URN)?.members.map((m) => m.entity.urn)).toEqual([
            MEMBERS[0].entity.urn,
            MEMBERS[1].entity.urn,
        ]);
    });

    it('sends one request for the same datasets in a different order, and keeps the groups meanwhile', async () => {
        const otherUrn = 'urn:li:dataset:(urn:li:dataPlatform:glue,db.payments,PROD)';
        const urns = [RESULT_URN, otherUrn].sort();
        const fetched = vi.fn(() => ({
            data: { siblingGroups: [fixtureGroup(MEMBERS.slice(0, 5), 7), fixtureGroup([], 0, otherUrn)] },
        }));
        const mocks: MockedResponse[] = [
            {
                request: { query: GetSiblingGroupsDocument, variables: { input: { urns, start: 0, count: 5 } } },
                result: fetched,
            },
        ];
        const { result, rerender, waitForNextUpdate } = renderHook(
            ({ order }: { order: string[] }) => useSiblingGroups(order, true),
            {
                initialProps: { order: [RESULT_URN, otherUrn] },
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename>
                        <TestPageContainer>{children}</TestPageContainer>
                    </MockedProvider>
                ),
            },
        );
        await waitForNextUpdate();
        rerender({ order: [otherUrn, RESULT_URN] });
        await settle();
        expect(fetched).toHaveBeenCalledTimes(1);
        expect(result.current.groups.get(RESULT_URN)?.members).toHaveLength(5);
        expect(result.current.error).toBeUndefined();
    });
});

describe('SearchResultSiblings', () => {
    function renderSiblings(mocks: MockedResponse[] = []) {
        return render(
            <MockedProvider mocks={mocks} addTypename>
                <TestPageContainer>
                    <SearchResultSiblings urn={RESULT_URN} resultName="orders" group={group(MEMBERS.slice(0, 5), 7)} />
                </TestPageContainer>
            </MockedProvider>,
        );
    }

    it('lists the first five with labels, the primary marked, then "+ N more"', () => {
        renderSiblings();
        expect(screen.getAllByTestId('sibling-row')).toHaveLength(5);
        expect(screen.getAllByText('Sibling of orders')).toHaveLength(5);
        expect(screen.getByText('Primary')).toBeInTheDocument();
        expect(screen.getByText('+ 2 more siblings of orders')).toBeInTheDocument();
    });

    it('expands to the whole group and collapses back', async () => {
        renderSiblings([siblingGroupsMock(0, EXPANDED_PAGE_SIZE)]);
        fireEvent.click(screen.getByText('+ 2 more siblings of orders'));

        await waitFor(() => expect(screen.getAllByTestId('sibling-compact-row')).toHaveLength(7));
        expect(screen.getByText('7 siblings of orders')).toBeInTheDocument();

        fireEvent.click(screen.getByText('Show less'));
        expect(screen.queryAllByTestId('sibling-compact-row')).toHaveLength(0);
        expect(screen.getAllByTestId('sibling-row')).toHaveLength(5);
    });

    it('points aria-controls at the rendered siblings', () => {
        renderSiblings();
        const more = screen.getByText('+ 2 more siblings of orders');
        const controlled = document.getElementById(more.getAttribute('aria-controls') || '');
        expect(controlled).toContainElement(screen.getAllByTestId('sibling-row')[0]);
    });
});
