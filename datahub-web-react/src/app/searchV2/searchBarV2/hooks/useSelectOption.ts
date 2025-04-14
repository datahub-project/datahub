import { useCallback } from 'react';
import { useHistory } from 'react-router';

import { EXACT_AUTOCOMPLETE_OPTION_TYPE, RELEVANCE_QUERY_OPTION_TYPE } from '@app/searchV2/searchBarV2/constants';
import filterSearchQuery from '@app/searchV2/utils/filterSearchQuery';
import analytics, { Event, EventType } from '@src/app/analytics';
import { getEntityPath } from '@src/app/entityV2/shared/containers/profile/utils';
import { isEntityType } from '@src/app/entityV2/shared/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { FacetFilterInput } from '@src/types.generated';
import { Option } from '@app/searchV2/searchBarV2/types';

export default function useSelectOption(
    onSearch: (query: string, filters?: FacetFilterInput[]) => void,
    onClear: () => void,
    filters: FacetFilterInput[],
) {
    const history = useHistory();
    const entityRegistry = useEntityRegistryV2();

    return useCallback(
        (value: string, option: Option) => {
            // If the autocomplete option type is NOT an entity, then render as a normal search query.
            if (option.type === EXACT_AUTOCOMPLETE_OPTION_TYPE || option.type === RELEVANCE_QUERY_OPTION_TYPE) {
                onSearch(`${filterSearchQuery(value)}`, filters);
                analytics.event({
                    type: EventType.SelectAutoCompleteOption,
                    optionType: option.type,
                } as Event);
            } else {
                if (!isEntityType(option.type)) return console.warn('Unknown entity type', option.type);

                // Navigate directly to the entity profile.
                history.push(getEntityPath(option.type, value, entityRegistry, false, false));
                onClear();
                analytics.event({
                    type: EventType.SelectAutoCompleteOption,
                    optionType: option.type,
                    entityType: option.type,
                    entityUrn: value,
                } as Event);
            }
        },
        [onSearch, onClear, entityRegistry, filters, history],
    );
}
