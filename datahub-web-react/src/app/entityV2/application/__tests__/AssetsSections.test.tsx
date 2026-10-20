import { render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { AssetsSection } from '@app/entityV2/application/AssetsSections';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));
vi.mock('@graphql/search.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/search.generated')>();
    return {
        ...actual,
        useGetSearchResultsForMultipleQuery: vi.fn(),
    };
});
vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: vi.fn(),
}));
vi.mock('react-i18next', async (importOriginal) => {
    const actual = await importOriginal<typeof import('react-i18next')>();
    return {
        ...actual,
        useTranslation: () => ({
            t: (key: string, opts?: { count?: number; type?: string }) =>
                opts?.count !== undefined ? `${key}:${opts.count}` : key,
        }),
    };
});

const theme = {
    colors: {
        text: '#000',
        textSecondary: '#666',
        textTertiary: '#999',
        bg: '#fff',
        border: '#ddd',
        icon: '#333',
        hyperlinks: '#1890ff',
    },
};

function renderWithProviders(ui: React.ReactElement) {
    return render(
        <ThemeProvider theme={theme as any}>
            <MemoryRouter>{ui}</MemoryRouter>
        </ThemeProvider>,
    );
}

describe('AssetsSection', () => {
    const urn = 'urn:li:application:payments';

    beforeEach(() => {
        vi.clearAllMocks();
        (useEntityData as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            urn,
            entityType: EntityType.Application,
        });
        (useEntityRegistry as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            getEntityName: (type: EntityType) => type,
            getEntityUrl: () => '/application/payments',
            getIcon: () => <span data-testid="entity-icon" />,
        });
    });

    it('requests facet summary with count 0 (no result cards)', () => {
        (useGetSearchResultsForMultipleQuery as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            loading: false,
            data: {
                searchAcrossEntities: {
                    total: 3,
                    searchResults: [],
                    facets: [
                        {
                            field: '_entityType',
                            aggregations: [{ value: 'DATASET', count: 3 }],
                        },
                    ],
                },
            },
        });

        renderWithProviders(<AssetsSection />);

        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        count: 0,
                        orFilters: [{ and: [{ field: 'applications', values: [urn] }] }],
                    }),
                }),
            }),
        );
        expect(screen.getByText(/shared.assetsCountTitle:3/)).toBeInTheDocument();
    });

    it('renders nothing when facet summary total is 0', () => {
        (useGetSearchResultsForMultipleQuery as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            loading: false,
            data: {
                searchAcrossEntities: {
                    total: 0,
                    searchResults: [],
                    facets: [],
                },
            },
        });

        const { container } = renderWithProviders(<AssetsSection />);
        expect(container).toBeEmptyDOMElement();
    });
});
