import React, { ReactNode, useEffect, useMemo } from 'react';
import SearchContext from './SearchContext';

type Props = {
    children: ReactNode;
    query: string;
    onChange: (any) => void;
};

const SearchProvider = ({ children, query, onChange }: Props) => {
    const value = useMemo(
        () => ({
            query,
        }),
        [query],
    );

    useEffect(() => {
        onChange({ field: 'query', value: query });
    }, [onChange, query]);

    return <SearchContext.Provider value={value}>{children}</SearchContext.Provider>;
};

export default SearchProvider;
