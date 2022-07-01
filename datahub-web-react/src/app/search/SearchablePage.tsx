import React, { useEffect } from 'react';
import { useHistory, useLocation } from 'react-router';
import * as QueryString from 'query-string';
import { useTheme } from 'styled-components';
import { SearchHeader } from './SearchHeader';
import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType } from '../../types.generated';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '../../graphql/search.generated';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import analytics, { EventType } from '../analytics';

const styles = {
    children: {
        flex: '1',
        marginTop: 60,
        display: 'flex',
        flexDirection: 'column' as const,
        maxHeight: 'calc(100vh - 60px)',
    },
};

interface Props extends React.PropsWithChildren<any> {
    onSearch?: (query: string, type?: EntityType) => void;
    onAutoComplete?: (query: string) => void;
}

const defaultProps = {
    onSearch: undefined,
    onAutoComplete: undefined,
};

/**
 * A page that includes a sticky search header (nav bar)
 */
export const SearchablePage = ({ onSearch, onAutoComplete, children }: Props) => {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const currentQuery: string = decodeURIComponent(params.query ? (params.query as string) : '');

    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const themeConfig = useTheme();

    const [getAutoCompleteResults, { data: suggestionsData }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const user = useGetAuthenticatedUser()?.corpUser;

    const search = (query: string, type?: EntityType) => {
        if (!query || query.trim().length === 0) {
            return;
        }
        analytics.event({
            type: EventType.SearchEvent,
            query,
            pageNumber: 1,
            originPath: window.location.pathname,
        });

        navigateToSearchUrl({
            type,
            query,
            history,
        });
    };

    const autoComplete = (query: string) => {
        if (query && query.trim() !== '') {
            getAutoCompleteResults({
                variables: {
                    input: {
                        query,
                    },
                },
            });
        }
    };

    // Load correct autocomplete results on initial page load.
    useEffect(() => {
        if (currentQuery && currentQuery.trim() !== '') {
            getAutoCompleteResults({
                variables: {
                    input: {
                        query: currentQuery,
                    },
                },
            });
        }
    }, [currentQuery, getAutoCompleteResults]);

    return (
        <>
            <SearchHeader
                initialQuery={currentQuery as string}
                placeholderText={themeConfig.content.search.searchbarMessage}
                suggestions={
                    (suggestionsData &&
                        suggestionsData?.autoCompleteForMultiple &&
                        suggestionsData.autoCompleteForMultiple.suggestions) ||
                    []
                }
                onSearch={onSearch || search}
                onQueryChange={onAutoComplete || autoComplete}
                authenticatedUserUrn={user?.urn || ''}
                authenticatedUserPictureLink={user?.editableProperties?.pictureLink}
                entityRegistry={entityRegistry}
            />
            <div style={styles.children}>{children}</div>
        </>
    );
};

SearchablePage.defaultProps = defaultProps;
