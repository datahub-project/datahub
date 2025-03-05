import { UnionType } from '@src/app/searchV2/utils/constants';
import { generateOrFilters } from '@src/app/searchV2/utils/generateOrFilters';
import useFilters from '@src/app/searchV2/utils/useFilters';
import { FacetFilterInput } from '@src/types.generated';
import * as QueryString from 'query-string';
import { useMemo } from 'react';
import { useLocation } from 'react-router';

export default function useGetActionRequestsQueryInputs() {
    const location = useLocation();

    const params = useMemo(() => {
        return QueryString.parse(location.search, { arrayFormat: 'comma' });
    }, [location.search]);

    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;
    const unionType: UnionType = Number(params.unionType as any as UnionType) || UnionType.AND;

    const filters: Array<FacetFilterInput> = useFilters(params);
    const orFilters = useMemo(() => generateOrFilters(unionType, filters), [filters, unionType]);

    return { unionType, orFilters, filters, page };
}
