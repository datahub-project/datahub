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
    selectedType?: EntityType;
    initialQuery?: string;
}

const defaultProps = {
    selectedType: undefined,
    initialQuery: '',
};

/**
 * A page that includes a sticky search header (nav bar)
 */
export const SearchablePage = ({ selectedType, initialQuery, children }: Props) => {
    const history = useHistory();

    const entityRegistry = useEntityRegistry();
    const searchTypes = entityRegistry.getSearchEntityTypes();
    const searchTypeNames = [
        ALL_ENTITIES_SEARCH_TYPE_NAME,
        ...searchTypes.map((entityType) => entityRegistry.getCollectionName(entityType)),
    ];

    const selectedSearchTypeName =
        selectedType && searchTypes.includes(selectedType)
            ? entityRegistry.getCollectionName(selectedType)
            : ALL_ENTITIES_SEARCH_TYPE_NAME;

    const [getAutoCompleteResults, { data: suggestionsData }] = useGetAutoCompleteResultsLazyQuery();

    const search = (typeName: string, query: string) => {
        navigateToSearchUrl({
            type:
                ALL_ENTITIES_SEARCH_TYPE_NAME === typeName
                    ? undefined
                    : entityRegistry.getTypeFromCollectionName(typeName),
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
                selectedType={selectedSearchTypeName}
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
