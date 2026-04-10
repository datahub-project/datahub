import { SimpleSelect } from '@components';
import React from 'react';

import { DEFAULT_SORT_OPTION } from '@app/searchV2/context/constants';
import useGetSortOptions from '@app/searchV2/sorting/useGetSortOptions';

type Props = {
    selectedSortOption: string | undefined;
    setSelectedSortOption: (option: string) => void;
};

export default function SearchSortSelect({ selectedSortOption, setSelectedSortOption }: Props) {
    const sortOptions = useGetSortOptions();
    const options = Object.entries(sortOptions).map(([value, option]) => ({ value, label: option.label }));

    const currentValue = selectedSortOption || DEFAULT_SORT_OPTION;

    return (
        <SimpleSelect
            options={options}
            values={[currentValue]}
            onUpdate={(values) => {
                if (values.length > 0) {
                    setSelectedSortOption(values[0]);
                }
            }}
            showClear={false}
            width="fit-content"
            size="md"
        />
    );
}
