import { Divider } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import FailingInputs from '@app/entity/shared/embed/UpstreamHealth/FailingInputs';
import { DATASET_COUNT, generateQueryVariables } from '@app/entity/shared/embed/UpstreamHealth/utils';
import { useGetDefaultLineageStartTimeMillis } from '@app/lineage/utils/useGetLineageTimeParams';
import { HAS_ACTIVE_INCIDENTS_FILTER_NAME, HAS_FAILING_ASSERTIONS_FILTER_NAME } from '@app/search/utils/constants';
import { useAppConfig } from '@app/useAppConfig';

import { useSearchAcrossLineageQuery } from '@graphql/search.generated';
import { Dataset } from '@types';

export const StyledDivider = styled(Divider)`
    margin: 16px 0;
`;

export default function UpstreamHealth() {
    const { entityData } = useEntityData();
    const [datasetsWithActiveIncidents, setDatasetsWithActiveIncidents] = useState<Dataset[]>([]);
    const [datasetsWithFailingAssertions, setDatasetsWithFailingAssertions] = useState<Dataset[]>([]);
    const [incidentsDataStart, setIncidentsDataStart] = useState(0);
    const [assertionsDataStart, setAssertionsDataStart] = useState(0);

    const appConfig = useAppConfig();
    const lineageEnabled: boolean = appConfig?.config?.chromeExtensionConfig?.lineageEnabled || false;
    const startTimeMillis = useGetDefaultLineageStartTimeMillis();

    const urn = entityData?.urn || '';

    const {
        data: incidentsData,
        loading: isLoadingIncidents,
        fetchMore: fetchMoreIncidents,
    } = useSearchAcrossLineageQuery(
        generateQueryVariables({
            urn,
            startTimeMillis,
            filterField: HAS_ACTIVE_INCIDENTS_FILTER_NAME,
            start: incidentsDataStart,
            includeAssertions: false,
            includeIncidents: true,
            skip: !lineageEnabled || !urn,
        }),
    );

    const {
        data: assertionsData,
        loading: isLoadingAssertions,
        fetchMore: fetchMoreAssertions,
    } = useSearchAcrossLineageQuery(
        generateQueryVariables({
            urn,
            startTimeMillis,
            filterField: HAS_FAILING_ASSERTIONS_FILTER_NAME,
            start: assertionsDataStart,
            includeAssertions: true,
            includeIncidents: false,
            skip: !lineageEnabled || !urn,
        }),
    );

    useEffect(() => {
        if (incidentsData?.searchAcrossLineage?.searchResults.length && !datasetsWithActiveIncidents.length) {
            setDatasetsWithActiveIncidents(
                incidentsData.searchAcrossLineage.searchResults.map((result) => result.entity as Dataset),
            );
        }
    }, [incidentsData?.searchAcrossLineage?.searchResults, datasetsWithActiveIncidents.length]);

    useEffect(() => {
        if (assertionsData?.searchAcrossLineage?.searchResults.length && !datasetsWithFailingAssertions.length) {
            setDatasetsWithFailingAssertions(
                assertionsData.searchAcrossLineage.searchResults.map((result) => result.entity as Dataset),
            );
        }
    }, [assertionsData?.searchAcrossLineage?.searchResults, datasetsWithFailingAssertions.length]);

    function fetchMoreIncidentsData() {
        const newIncidentsStart = incidentsDataStart + DATASET_COUNT;
        fetchMoreIncidents(
            generateQueryVariables({
                urn,
                startTimeMillis,
                filterField: HAS_ACTIVE_INCIDENTS_FILTER_NAME,
                start: newIncidentsStart,
                includeAssertions: false,
                includeIncidents: true,
            }),
        ).then((result) => {
            if (result.data.searchAcrossLineage?.searchResults) {
                setDatasetsWithActiveIncidents([
                    ...datasetsWithActiveIncidents,
                    ...result.data.searchAcrossLineage.searchResults.map((r) => r.entity as Dataset),
                ]);
            }
        });
        setIncidentsDataStart(newIncidentsStart);
    }

    function fetchMoreAssertionsData() {
        const newAssertionsStart = assertionsDataStart + DATASET_COUNT;
        fetchMoreAssertions(
            generateQueryVariables({
                urn,
                startTimeMillis,
                filterField: HAS_FAILING_ASSERTIONS_FILTER_NAME,
                start: newAssertionsStart,
                includeAssertions: true,
                includeIncidents: false,
            }),
        ).then((result) => {
            if (result.data.searchAcrossLineage?.searchResults) {
                setDatasetsWithFailingAssertions([
                    ...datasetsWithFailingAssertions,
                    ...result.data.searchAcrossLineage.searchResults.map((r) => r.entity as Dataset),
                ]);
            }
        });
        setAssertionsDataStart(newAssertionsStart);
    }

    const hasUnhealthyUpstreams = datasetsWithActiveIncidents.length || datasetsWithFailingAssertions.length;

    if (!hasUnhealthyUpstreams) return null;

    return (
        <>
            <FailingInputs
                datasetsWithActiveIncidents={datasetsWithActiveIncidents as Dataset[]}
                totalDatasetsWithActiveIncidents={incidentsData?.searchAcrossLineage?.total || 0}
                fetchMoreIncidentsData={fetchMoreIncidentsData}
                isLoadingIncidents={isLoadingIncidents}
                datasetsWithFailingAssertions={datasetsWithFailingAssertions as Dataset[]}
                totalDatasetsWithFailingAssertions={assertionsData?.searchAcrossLineage?.total || 0}
                fetchMoreAssertionsData={fetchMoreAssertionsData}
                isLoadingAssertions={isLoadingAssertions}
            />
            <StyledDivider />
        </>
    );
}
