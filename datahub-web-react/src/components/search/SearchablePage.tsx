import * as React from 'react';
import 'antd/dist/antd.css';
import { Layout } from 'antd';
import { useHistory } from 'react-router';
import { SearchHeader } from './SearchHeader';
import { EntityType, fromCollectionName, toCollectionName, toPathName } from '../shared/EntityTypeUtil';
import { SearchCfg } from '../../conf';
import { useGetAutoCompleteResultsLazyQuery } from '../../graphql/search.generated';

const { SEARCHABLE_ENTITY_TYPES, SEARCH_BAR_PLACEHOLDER_TEXT, SHOW_ALL_ENTITIES_SEARCH_TYPE } = SearchCfg;

const ALL_ENTITIES_SEARCH_TYPE_NAME = 'All Entities';
const EMPTY_STRING = '';

const DEFAULT_SELECTED_ENTITY_TYPE_NAME = SHOW_ALL_ENTITIES_SEARCH_TYPE
    ? ALL_ENTITIES_SEARCH_TYPE_NAME
    : toCollectionName(SEARCHABLE_ENTITY_TYPES[0]);

const SUPPORTED_SEARCH_TYPE_NAMES = SHOW_ALL_ENTITIES_SEARCH_TYPE
    ? [ALL_ENTITIES_SEARCH_TYPE_NAME, ...SEARCHABLE_ENTITY_TYPES.map((entityType) => toCollectionName(entityType))]
    : [...SEARCHABLE_ENTITY_TYPES.map((entityType) => toCollectionName(entityType))];

interface Props extends React.PropsWithChildren<any> {
    initialType?: EntityType;
    initialQuery?: string;
}

const defaultProps = {
    initialType: undefined,
    initialQuery: EMPTY_STRING,
};

/**
 * A page that includes a sticky search header (nav bar)
 */
export const SearchablePage = ({
    initialType: _initialType,
    initialQuery: _initialQuery,
    children: _children,
}: Props) => {
    const history = useHistory();

    const initialSearchTypeName = _initialType ? toCollectionName(_initialType) : DEFAULT_SELECTED_ENTITY_TYPE_NAME;

    if (!SUPPORTED_SEARCH_TYPE_NAMES.includes(initialSearchTypeName)) {
        throw new Error(`Unsupported search EntityType ${_initialType} provided!`);
    }

    const [getAutoCompleteResults, { data: suggestionsData }] = useGetAutoCompleteResultsLazyQuery();

    const search = (type: string, query: string) => {
        const typeParam =
            ALL_ENTITIES_SEARCH_TYPE_NAME === type ? EMPTY_STRING : `type=${toPathName(fromCollectionName(type))}`;
        const queryParam = `query=${query}`;

        history.push({
            pathname: '/search',
            search: `?${typeParam}&${queryParam}`,
        });
    };

    const autoComplete = (type: string, query: string) => {
        const entityType =
            ALL_ENTITIES_SEARCH_TYPE_NAME === type ? SEARCHABLE_ENTITY_TYPES[0] : fromCollectionName(type);
        const autoCompleteField = SearchCfg.getAutoCompleteFieldName(entityType);

        if (autoCompleteField) {
            getAutoCompleteResults({
                variables: {
                    input: {
                        type: entityType,
                        query,
                        field: autoCompleteField,
                    },
                },
            });
        }
    };

    return (
        <Layout>
            <SearchHeader
                types={SUPPORTED_SEARCH_TYPE_NAMES}
                initialType={initialSearchTypeName}
                initialQuery={_initialQuery as string}
                placeholderText={SEARCH_BAR_PLACEHOLDER_TEXT}
                suggestions={
                    (suggestionsData && suggestionsData?.autoComplete && suggestionsData.autoComplete.suggestions) || []
                }
                onSearch={search}
                onQueryChange={autoComplete}
                authenticatedUserUrn="urn:li:corpuser:0"
            />
            <div style={{ marginTop: 64 }}>{_children}</div>
        </Layout>
    );
};

SearchablePage.defaultProps = defaultProps;
