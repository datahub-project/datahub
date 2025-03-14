import { EntityType } from '@src/types.generated';
import { useMemo } from 'react';
import { isItDomainEntity } from '@src/app/entityV2/domain/utils';
import useAutocompleteResults from '../../BaseEntityFilter/hooks/useAutocompleteResults';

const ENTITY_TYPES = [EntityType.Domain];

export default function useDomainsFromSuggestions(query: string) {
    const { data, loading } = useAutocompleteResults(query, ENTITY_TYPES);

    const domains = useMemo(
        () =>
            (data?.autoCompleteForMultiple?.suggestions || [])
                .flatMap((suggestion) => suggestion.entities)
                .filter(isItDomainEntity),
        [data?.autoCompleteForMultiple?.suggestions],
    );

    return { domains, loading };
}
