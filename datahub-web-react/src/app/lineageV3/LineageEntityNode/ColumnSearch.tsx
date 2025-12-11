/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
