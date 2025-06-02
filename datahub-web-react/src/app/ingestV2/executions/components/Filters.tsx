import * as QueryString from 'query-string';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import ExecutorTypeFilter from '@app/ingestV2/shared/components/filters/ExecutorTypeFilter';
import SourceFilter from '@app/ingestV2/shared/components/filters/SourceFilter';
import filtersToQueryStringParams from '@app/searchV2/utils/filtersToQueryStringParams';
import useFilters from '@app/searchV2/utils/useFilters';

import { FacetFilterInput } from '@types';

export const INGESTION_SOURCE_FIELD = 'ingestionSource';
export const EXECUTOR_TYPE_FIELD = 'executorType';

const Container = styled.div`
    display: flex;
    gap: 8px;
    padding: 1px; // prevent cutting of select's outline
`;

export type FiltersState = Map<string, string[]>;

interface Props {
    onFiltersApplied?: (filters: FiltersState) => void;
    hideSystemSources: boolean;
}

export default function Filters({ onFiltersApplied, hideSystemSources }: Props) {
    const location = useLocation();
    const history = useHistory();
    // initialization of query params from location
    const queryParams = useState<QueryString.ParsedQuery<string>>(
        QueryString.parse(location.search, { arrayFormat: 'comma' }),
    )[0];
    const paramFilters: Array<FacetFilterInput> = useFilters(queryParams);
    const defaultValues = useMemo(
        () => new Map<string, string[]>(paramFilters.map((item) => [item.field, item.values ?? []])),
        [paramFilters],
    );
    const [valuesMap, setValuesMap] = useState<FiltersState>(defaultValues);

    const [isInitialized, setIsInitialised] = useState<boolean>(false);

    useEffect(() => {
        if (!isInitialized) {
            onFiltersApplied?.(defaultValues);
            setIsInitialised(true);
        }
    }, [defaultValues, onFiltersApplied, isInitialized]);

    const onUpdate = useCallback(
        (field: string, values: string[]) => {
            const newValuesMap = new Map([...valuesMap.entries(), [field, values]]);

            setValuesMap(newValuesMap);
            onFiltersApplied?.(newValuesMap);

            const search = QueryString.stringify(
                {
                    ...filtersToQueryStringParams(
                        Array.from(newValuesMap.entries()).map(([field_, values_]) => ({
                            field: field_,
                            values: values_,
                        })),
                    ),
                },
                { arrayFormat: 'comma' },
            );

            history.push({
                pathname: location.pathname,
                search,
            });
        },
        [valuesMap, onFiltersApplied, history, location.pathname],
    );

    return (
        <Container>
            <SourceFilter
                defaultValues={defaultValues.get(INGESTION_SOURCE_FIELD)}
                onUpdate={(values) => onUpdate(INGESTION_SOURCE_FIELD, values)}
                hideSystemSources={!!hideSystemSources}
            />
            <ExecutorTypeFilter onUpdate={(values) => onUpdate(EXECUTOR_TYPE_FIELD, values)} />
        </Container>
    );
}
