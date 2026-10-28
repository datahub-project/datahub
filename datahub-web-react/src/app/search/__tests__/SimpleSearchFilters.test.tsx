import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { SimpleSearchFilters } from '@app/search/SimpleSearchFilters';

import { FacetMetadata } from '@types';

vi.mock('@app/useAppConfig', () => ({ useAppConfig: () => ({ config: {} }) }));
vi.mock('@app/search/filters/render/useFilterRenderer', () => ({
    useFilterRendererRegistry: () => ({ hasRenderer: () => false }),
}));
vi.mock('@app/search/SimpleSearchFilter', () => ({
    SimpleSearchFilter: ({ facet }: { facet: FacetMetadata }) => <div data-testid="facet">{facet.field}</div>,
}));

const makeFacet = (field: string): FacetMetadata => ({
    field,
    displayName: 'Environment',
    aggregations: [{ value: 'PROD', count: 3 }],
});

const renderedFields = () => screen.queryAllByTestId('facet').map((el) => el.textContent);

describe('SimpleSearchFilters', () => {
    it('shows the env facet as origin when origin is not returned', () => {
        render(
            <SimpleSearchFilters
                facets={[makeFacet('env')]}
                selectedFilters={[]}
                onFilterSelect={vi.fn()}
                loading={false}
            />,
        );

        expect(renderedFields()).toEqual(['origin']);
    });

    it('shows a single environment facet when both env and origin are returned', () => {
        render(
            <SimpleSearchFilters
                facets={[makeFacet('origin'), makeFacet('env')]}
                selectedFilters={[]}
                onFilterSelect={vi.fn()}
                loading={false}
            />,
        );

        expect(renderedFields()).toEqual(['origin']);
    });
});
