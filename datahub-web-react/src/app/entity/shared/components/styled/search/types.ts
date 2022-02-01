import {
    Entity,
    MatchedField,
    Maybe,
    SearchAcrossEntitiesInput,
    SearchInsight,
} from '../../../../../../types.generated';

export type GetSearchResultsParams = {
    variables: {
        input: SearchAcrossEntitiesInput;
    };
} & Record<string, any>;

export type SearchResultInterface = {
    entity: Entity;
    /** Insights about why the search result was matched */
    insights?: Maybe<Array<SearchInsight>>;
    /** Matched field hint */
    matchedFields: Array<MatchedField>;
    paths?: Array<Entity>;
} & Record<string, any>;
