import React, { useEffect } from 'react';
import { useHistory } from 'react-router';
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
    },
};

interface Props extends React.PropsWithChildren<any> {
    initialQuery?: string;
    onSearch?: (query: string, type?: EntityType) => void;
    onAutoComplete?: (query: string) => void;
}

const defaultProps = {
    initialQuery: '',
    onSearch: undefined,
    onAutoComplete: undefined,
};

/**
 * A page that includes a sticky search header (nav bar)
 */
export const SearchablePage = ({ initialQuery, onSearch, onAutoComplete, children }: Props) => {
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
        if (initialQuery && initialQuery.trim() !== '') {
            getAutoCompleteResults({
                variables: {
                    input: {
                        query: initialQuery,
                    },
                },
            });
        }
    }, [initialQuery, getAutoCompleteResults]);

    return (
        <>
            <SearchHeader
                initialQuery={initialQuery as string}
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
