import React from 'react';
import 'antd/dist/antd.css';
import { Layout } from 'antd';
import { useHistory } from 'react-router';
import { SearchHeader } from './SearchHeader';
import { SearchCfg } from '../../conf';
import { useEntityRegistry } from '../useEntityRegistry';
import { useGetAutoCompleteResultsLazyQuery } from '../../graphql/search.generated';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';

const styles = {
    pageContainer: { backgroundColor: '#FFFFFF' },
    children: { marginTop: 80 },
};

interface Props extends React.PropsWithChildren<any> {
    initialQuery?: string;
    onSearch?: (query: string) => void;
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

    const { data: userData } = useGetAuthenticatedUser();
    const [getAutoCompleteResults, { data: suggestionsData }] = useGetAutoCompleteResultsLazyQuery();

    const search = (query: string) => {
        navigateToSearchUrl({
            query,
            history,
            entityRegistry,
        });
    };

    const autoComplete = (query: string) => {
        getAutoCompleteResults({
            variables: {
                input: {
                    type: entityRegistry.getDefaultSearchEntityType(),
                    query,
                },
            },
        });
    };

    return (
        <Layout style={styles.pageContainer}>
            <SearchHeader
                initialQuery={initialQuery as string}
                placeholderText={SearchCfg.SEARCH_BAR_PLACEHOLDER_TEXT}
                suggestions={
                    (suggestionsData && suggestionsData?.autoComplete && suggestionsData.autoComplete.suggestions) || []
                }
                onSearch={onSearch || search}
                onQueryChange={onAutoComplete || autoComplete}
                authenticatedUserUrn={userData?.corpUser?.urn || ''}
                authenticatedUserPictureLink={userData?.corpUser?.editableInfo?.pictureLink}
            />
            <div style={styles.children}>{children}</div>
        </Layout>
    );
};

SearchablePage.defaultProps = defaultProps;
