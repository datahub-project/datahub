import { render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { TagAppliedToColumn } from '@app/tags/TagsTableColumns';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';

vi.mock('@graphql/search.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/search.generated')>();
    return {
        ...actual,
        useGetSearchResultsForMultipleQuery: vi.fn(),
    };
});
vi.mock('@graphql/tag.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/tag.generated')>();
    return {
        ...actual,
        useGetTagQuery: vi.fn(() => ({ data: undefined, loading: false })),
    };
});
vi.mock('@graphql/mutations.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/mutations.generated')>();
    return {
        ...actual,
        useBatchUpdateDeprecationMutation: vi.fn(() => [vi.fn()]),
    };
});
vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: vi.fn(),
    useEntityRegistryV2: vi.fn(),
}));
vi.mock('@src/app/useEntityRegistry', () => ({
    useEntityRegistry: vi.fn(),
    useEntityRegistryV2: vi.fn(),
}));
vi.mock('react-i18next', async (importOriginal) => {
    const actual = await importOriginal<typeof import('react-i18next')>();
    return {
        ...actual,
        useTranslation: () => ({
            t: (key: string, opts?: { count?: number }) => (opts?.count !== undefined ? `${key}:${opts.count}` : key),
        }),
        Trans: ({ i18nKey }: { i18nKey: string }) => <span>{i18nKey}</span>,
    };
});

const theme = {
    colors: {
        text: '#000',
        textSecondary: '#666',
        hyperlinks: '#1890ff',
        bg: '#fff',
        border: '#ddd',
    },
};

describe('TagAppliedToColumn', () => {
    const tagUrn = 'urn:li:tag:perf_seed_tag_1';

    beforeEach(() => {
        vi.clearAllMocks();
        const registry = {
            getCollectionName: (type: string) => type,
        };
        (useEntityRegistry as unknown as ReturnType<typeof vi.fn>).mockReturnValue(registry);
    });

    it('requests entity facet aggregations with count 0', () => {
        (useGetSearchResultsForMultipleQuery as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            loading: false,
            data: {
                searchAcrossEntities: {
                    total: 10,
                    searchResults: [],
                    facets: [
                        {
                            field: 'entity',
                            aggregations: [
                                { value: 'DATASET', count: 8 },
                                { value: 'DASHBOARD', count: 2 },
                            ],
                        },
                    ],
                },
            },
        });

        render(
            <ThemeProvider theme={theme as any}>
                <MemoryRouter>
                    <TagAppliedToColumn tagUrn={tagUrn} />
                </MemoryRouter>
            </ThemeProvider>,
        );

        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        count: 0,
                    }),
                }),
            }),
        );
        expect(screen.getByText('tags.appliedToEntityCount:10')).toBeInTheDocument();
    });
});
