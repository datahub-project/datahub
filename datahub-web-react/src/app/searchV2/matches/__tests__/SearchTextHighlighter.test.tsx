import { render, screen } from '@testing-library/react';
import React from 'react';

import { SearchContext } from '@app/search/context/SearchContext';
import { SearchResultProvider } from '@app/search/context/SearchResultContext';
import SearchTextHighlighter from '@app/searchV2/matches/SearchTextHighlighter';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { AppConfigContext, DEFAULT_APP_CONFIG } from '@src/appConfigContext';

import { EntityType } from '@types';

const appConfig = {
    config: {
        ...DEFAULT_APP_CONFIG,
        visualConfig: {
            ...DEFAULT_APP_CONFIG.visualConfig,
            searchResult: {
                ...DEFAULT_APP_CONFIG.visualConfig.searchResult,
                enableNameHighlight: true,
            },
        },
    },
    loaded: true,
    refreshContext: () => {},
};

function renderHighlighter(query: string, text: string) {
    return render(
        <AppConfigContext.Provider value={appConfig}>
            <CustomThemeProvider>
                <SearchContext.Provider
                    value={{
                        query,
                        selectedSortOption: undefined,
                        isFullViewCard: false,
                        setSelectedSortOption: () => null,
                        setIsFullViewCard: () => null,
                    }}
                >
                    <SearchResultProvider
                        searchResult={
                            {
                                entity: {
                                    urn: 'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),my_field)',
                                    type: EntityType.SchemaField,
                                },
                                matchedFields: [{ name: 'id', value: 'my_field' }],
                            } as any
                        }
                    >
                        <SearchTextHighlighter field="name" text={text} />
                    </SearchResultProvider>
                </SearchContext.Provider>
            </CustomThemeProvider>
        </AppConfigContext.Provider>,
    );
}

describe('SearchTextHighlighter', () => {
    it('highlights a title when the match is on id and the query is double quoted', () => {
        renderHighlighter('"my_field"', 'orders.my_field');

        expect(screen.getByText('my_field').tagName).toBe('MARK');
        expect(screen.getByText('orders.')).toBeInTheDocument();
    });

    it('highlights a title when the match is on id and the query is single quoted', () => {
        renderHighlighter("'my_field'", 'orders.my_field');

        expect(screen.getByText('my_field').tagName).toBe('MARK');
        expect(screen.getByText('orders.')).toBeInTheDocument();
    });

    it('highlights a title that contains a quoted multi-term query', () => {
        renderHighlighter('"quarterly" \'revenue\'', 'quarterly revenue report');

        expect(screen.getByText('quarterly revenue').tagName).toBe('MARK');
    });

    it('keeps an apostrophe inside a word', () => {
        renderHighlighter("it's", "it's mine");

        expect(screen.getByText("it's").tagName).toBe('MARK');
    });
});
