import { SearchBar } from '@components';
import React, { Dispatch, SetStateAction, useState } from 'react';
import { useDebounce } from 'react-use';

import { onClickPreventSelect } from '@app/lineageV3/common';

interface Props {
    searchText: string;
    setSearchText: Dispatch<SetStateAction<string>>;
}

export default function ColumnSearch({ searchText, setSearchText }: Props) {
    // Prevent lag with local value
    const [value, setValue] = useState(searchText);

    useDebounce(() => setSearchText(value), 200, [value]);

    return (
        <SearchBar
            value={value}
            defaultValue={searchText}
            placeholder="Find column"
            onChange={(v) => setValue(v.trim())}
            onClick={onClickPreventSelect}
            height="32px"
        />
    );
}
