import { Divider } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useSearchAcrossLineageQuery } from '../../../../../graphql/search.generated';
import { Dataset } from '../../../../../types.generated';
import {
    HAS_ACTIVE_INCIDENTS_FILTER_NAME,
    HAS_FAILING_ASSERTIONS_FILTER_NAME,
} from '../../../../search/utils/constants';
import { useAppConfig } from '../../../../useAppConfig';
import { useEntityData } from '../../EntityContext';
import FailingInputs from './FailingInputs';
import { DATASET_COUNT, generateQueryVariables } from './utils';

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

    const urn = entityData?.urn || '';

    const {
        data: incidentsData,
        loading: isLoadingIncidents,
        fetchMore: fetchMoreIncidents,
    } = useSearchAcrossLineageQuery(
        generateQueryVariables(urn, HAS_ACTIVE_INCIDENTS_FILTER_NAME, incidentsDataStart, false, true, !lineageEnabled),
    );

    const {
        data: assertionsData,
        loading: isLoadingAssertions,
        fetchMore: fetchMoreAssertions,
    } = useSearchAcrossLineageQuery(
        generateQueryVariables(
            urn,
            HAS_FAILING_ASSERTIONS_FILTER_NAME,
            assertionsDataStart,
            true,
            false,
            !lineageEnabled,
        ),
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
        fetchMoreIncidents(generateQueryVariables(urn, 'hasActiveIncidents', newIncidentsStart, false, true)).then(
            (result) => {
                if (result.data.searchAcrossLineage?.searchResults) {
                    setDatasetsWithActiveIncidents([
                        ...datasetsWithActiveIncidents,
                        ...result.data.searchAcrossLineage.searchResults.map((r) => r.entity as Dataset),
                    ]);
                }
            },
        );
        setIncidentsDataStart(newIncidentsStart);
    }

    function fetchMoreAssertionsData() {
        const newAssertionsStart = assertionsDataStart + DATASET_COUNT;
        fetchMoreAssertions(generateQueryVariables(urn, 'hasFailingAssertions', newAssertionsStart, true, false)).then(
            (result) => {
                if (result.data.searchAcrossLineage?.searchResults) {
                    setDatasetsWithFailingAssertions([
                        ...datasetsWithFailingAssertions,
                        ...result.data.searchAcrossLineage.searchResults.map((r) => r.entity as Dataset),
                    ]);
                }
            },
        );
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
