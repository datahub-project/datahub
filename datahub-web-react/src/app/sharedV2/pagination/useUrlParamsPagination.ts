import { useCallback, useMemo } from 'react';

import usePagination, { Pagination } from '@app/sharedV2/pagination/usePagination';
import { useUrlQueryParam } from '@app/sharedV2/useUrlQueryParam';

export default function useUrlParamsPagination(defaultPageSize?: number) {
    const { value: urlParamPage, setValue: setUrlParamPage } = useUrlQueryParam('page', '1');

    const urlPage = useMemo(() => Number(urlParamPage), [urlParamPage]);
    const setUrlPage = useCallback((newPage: number) => setUrlParamPage(newPage.toString()), [setUrlParamPage]);

    const { page, setPage, pageSize, setPageSize, start, count } = usePagination(defaultPageSize, urlPage);

    const onSetPage = useCallback(
        (newPage: number) => {
            setUrlPage(newPage);
            setPage(newPage);
        },
        [setUrlPage, setPage],
    );

    return { page, setPage: onSetPage, pageSize, setPageSize, start, count } as Pagination;
}
