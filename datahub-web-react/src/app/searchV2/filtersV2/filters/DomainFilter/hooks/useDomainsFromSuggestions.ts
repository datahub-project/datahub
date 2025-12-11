/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import useAutocompleteResults from '@app/searchV2/filtersV2/filters/BaseEntityFilter/hooks/useAutocompleteResults';
import { isDomain } from '@src/app/entityV2/domain/utils';
import { EntityType } from '@src/types.generated';

const ENTITY_TYPES = [EntityType.Domain];

export default function useDomainsFromSuggestions(query: string) {
    const { data, loading } = useAutocompleteResults(query, ENTITY_TYPES);

    const domains = useMemo(
        () =>
            (data?.autoCompleteForMultiple?.suggestions || [])
                .flatMap((suggestion) => suggestion.entities)
                .filter(isDomain),
        [data?.autoCompleteForMultiple?.suggestions],
    );

    return { domains, loading };
}
