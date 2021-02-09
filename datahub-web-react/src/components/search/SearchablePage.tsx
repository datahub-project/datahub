import React from 'react';
import 'antd/dist/antd.css';
import { Layout } from 'antd';
import { useHistory } from 'react-router';
import { SearchHeader } from './SearchHeader';
import { SearchCfg } from '../../conf';
import { useEntityRegistry } from '../useEntityRegistry';
import { useGetAutoCompleteResultsLazyQuery } from '../../graphql/search.generated';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { EntityType } from '../../types.generated';

const ALL_ENTITIES_SEARCH_TYPE_NAME = 'All Entities';

interface Props extends React.PropsWithChildren<any> {
    initialType?: EntityType;
    initialQuery?: string;
}

const defaultProps = {
    initialType: undefined,
    initialQuery: '',
};

/**
 * A page that includes a sticky search header (nav bar)
 */
export const SearchablePage = ({ initialType, initialQuery, children }: Props) => {
    const history = useHistory();

    const entityRegistry = useEntityRegistry();
    const searchTypes = entityRegistry.getSearchEntityTypes();
    const searchTypeNames = searchTypes.map((entityType) => entityRegistry.getCollectionName(entityType));

    const initialSearchTypeName =
        initialType && searchTypes.includes(initialType)
            ? entityRegistry.getCollectionName(initialType)
            : ALL_ENTITIES_SEARCH_TYPE_NAME;

    const [getAutoCompleteResults, { data: suggestionsData }] = useGetAutoCompleteResultsLazyQuery();

    const search = (type: string, query: string) => {
        navigateToSearchUrl({
            type:
                ALL_ENTITIES_SEARCH_TYPE_NAME === type
                    ? searchTypes[0]
                    : entityRegistry.getTypeFromCollectionName(type),
            query,
            history,
            entityRegistry,
        });
    };

    const autoComplete = (type: string, query: string) => {
        const entityType =
            ALL_ENTITIES_SEARCH_TYPE_NAME === type ? searchTypes[0] : entityRegistry.getTypeFromCollectionName(type);
        getAutoCompleteResults({
            variables: {
                input: {
                    type: entityType,
                    query,
                },
            },
        });
    };

    return (
        <Layout>
            <SearchHeader
                types={searchTypeNames}
                initialType={initialSearchTypeName}
                initialQuery={initialQuery as string}
                placeholderText={SearchCfg.SEARCH_BAR_PLACEHOLDER_TEXT}
                suggestions={
                    (suggestionsData && suggestionsData?.autoComplete && suggestionsData.autoComplete.suggestions) || []
                }
                onSearch={search}
                onQueryChange={autoComplete}
                authenticatedUserUrn="urn:li:corpuser:0"
            />
            <div style={{ marginTop: 64 }}>{children}</div>
        </Layout>
    );
};

SearchablePage.defaultProps = defaultProps;
