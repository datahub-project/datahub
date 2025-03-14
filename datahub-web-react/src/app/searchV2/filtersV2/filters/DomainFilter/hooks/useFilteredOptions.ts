import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { filterNestedSelectOptions } from '@src/alchemy-components/components/Select/Nested/utils';
import { useMemo } from 'react';
import { isItDomainEntity } from '@src/app/entityV2/domain/utils';

export default function useFilteredOptions(options: SelectOption[], query: string) {
    return useMemo(
        () =>
            filterNestedSelectOptions(options, query, (option) => {
                const { entity } = option;
                if (!isItDomainEntity(entity)) return false;

                const searchText = (entity.properties?.name ?? '').toLowerCase();
                return (
                    searchText.includes(query.toLowerCase()) || entity.urn.toLowerCase().includes(query.toLowerCase())
                );
            }),
        [options, query],
    );
}
