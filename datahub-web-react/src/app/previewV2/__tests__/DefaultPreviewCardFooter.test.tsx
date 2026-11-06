import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { HelmetProvider } from 'react-helmet-async';
import { MemoryRouter } from 'react-router';

import { EntityCapabilityType } from '@app/entityV2/Entity';
import PreviewContext from '@app/entityV2/shared/PreviewContext';
import { DatasetLastUpdatedMs } from '@app/entityV2/shared/utils';
import DefaultPreviewCardFooter from '@app/previewV2/DefaultPreviewCardFooter';
import { SearchResultProvider } from '@app/search/context/SearchResultContext';
import { SearchResultLineageStatusProvider } from '@app/searchV2/SearchResultLineageStatusContext';
import { hexToRgb } from '@app/sharedV2/colors/colorUtils';
import lightTheme from '@conf/theme/colorThemes/light';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityRegistryContext } from '@src/entityRegistryContext';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

const URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)';

const searchResult = {
    entity: { urn: URN, type: EntityType.Dataset },
    matchedFields: [],
};

function renderFooter(
    previewData: Record<string, unknown> | null,
    {
        asSearchResult = true,
        lineageCountsFailed = false,
        lastUpdatedMs,
    }: { asSearchResult?: boolean; lineageCountsFailed?: boolean; lastUpdatedMs?: DatasetLastUpdatedMs } = {},
) {
    const entityRegistry = getTestEntityRegistry();
    const footer = (
        <DefaultPreviewCardFooter
            entityCapabilities={new Set([EntityCapabilityType.LINEAGE])}
            entityType={EntityType.Dataset}
            urn={URN}
            entityRegistry={entityRegistry}
            isFullViewCard
            lastUpdatedMs={lastUpdatedMs}
        />
    );
    const withPreview = (
        <PreviewContext.Provider value={{ previewData: previewData as any }}>{footer}</PreviewContext.Provider>
    );
    const withLineageStatus = (
        <SearchResultLineageStatusProvider failed={lineageCountsFailed}>
            {withPreview}
        </SearchResultLineageStatusProvider>
    );
    const withSearch = asSearchResult ? (
        <SearchResultProvider searchResult={searchResult as any}>{withLineageStatus}</SearchResultProvider>
    ) : (
        withLineageStatus
    );

    return render(
        <HelmetProvider>
            <CustomThemeProvider>
                <MemoryRouter>
                    <MockedProvider mocks={[]}>
                        <EntityRegistryContext.Provider value={entityRegistry}>
                            {withSearch}
                        </EntityRegistryContext.Provider>
                    </MockedProvider>
                </MemoryRouter>
            </CustomThemeProvider>
        </HelmetProvider>,
    );
}

describe('DefaultPreviewCardFooter lineage badge', () => {
    it('reserves the lineage badge slot on search cards until counts arrive', () => {
        const { rerender } = renderFooter({ urn: URN, type: EntityType.Dataset });

        expect(screen.getByTestId('lineage-badge-placeholder')).toBeInTheDocument();
        expect(screen.queryByRole('link')).not.toBeInTheDocument();

        const entityRegistry = getTestEntityRegistry();
        rerender(
            <HelmetProvider>
                <CustomThemeProvider>
                    <MemoryRouter>
                        <MockedProvider mocks={[]}>
                            <EntityRegistryContext.Provider value={entityRegistry}>
                                <SearchResultProvider searchResult={searchResult as any}>
                                    <PreviewContext.Provider
                                        value={{
                                            previewData: {
                                                urn: URN,
                                                type: EntityType.Dataset,
                                                upstream: { total: 2, filtered: 0 },
                                                downstream: { total: 1, filtered: 0 },
                                            } as any,
                                        }}
                                    >
                                        <DefaultPreviewCardFooter
                                            entityCapabilities={new Set([EntityCapabilityType.LINEAGE])}
                                            entityType={EntityType.Dataset}
                                            urn={URN}
                                            entityRegistry={entityRegistry}
                                            isFullViewCard
                                        />
                                    </PreviewContext.Provider>
                                </SearchResultProvider>
                            </EntityRegistryContext.Provider>
                        </MockedProvider>
                    </MemoryRouter>
                </CustomThemeProvider>
            </HelmetProvider>,
        );

        expect(screen.queryByTestId('lineage-badge-placeholder')).not.toBeInTheDocument();
        expect(screen.getByRole('link')).toHaveAttribute('href', expect.stringContaining('/Lineage'));
    });

    it('shows the lineage badge on non-search previews without deferred counts', () => {
        renderFooter({ urn: URN, type: EntityType.Dataset }, { asSearchResult: false });

        expect(screen.queryByTestId('lineage-badge-placeholder')).not.toBeInTheDocument();
        expect(document.querySelector('svg')).toBeTruthy();
    });

    it('clears the reserved lineage badge slot when the deferred count batch fails', () => {
        renderFooter({ urn: URN, type: EntityType.Dataset }, { lineageCountsFailed: true });

        expect(screen.queryByTestId('lineage-badge-placeholder')).not.toBeInTheDocument();
        expect(screen.queryByRole('link')).not.toBeInTheDocument();
    });
});

describe('DefaultPreviewCardFooter freshness clock', () => {
    const SIXTY_DAYS_AGO = Date.now() - 60 * 24 * 60 * 60 * 1000;
    const AGE_RED = 'rgb(245, 34, 45)';
    const NEUTRAL = `rgb(${hexToRgb(lightTheme.icon).join(', ')})`;
    const clockColor = () => getComputedStyle(screen.getByTestId('freshness').querySelector('svg') as SVGElement).color;
    const preview = { urn: URN, type: EntityType.Dataset };

    it('shows the time of a view in the neutral icon color', () => {
        renderFooter(preview, {
            lastUpdatedMs: { property: 'lastModified', lastUpdatedMs: SIXTY_DAYS_AGO, definitionTime: true },
        });
        expect(clockColor()).toBe(NEUTRAL);
    });

    it('colors an old table time by its age', () => {
        renderFooter(preview, {
            lastUpdatedMs: { property: 'lastModified', lastUpdatedMs: SIXTY_DAYS_AGO, definitionTime: false },
        });
        expect(clockColor()).toBe(AGE_RED);
    });
});
