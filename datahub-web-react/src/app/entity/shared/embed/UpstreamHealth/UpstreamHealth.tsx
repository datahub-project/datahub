import { Divider } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useSearchAcrossLineageQuery } from '../../../../../graphql/search.generated';
import { Dataset } from '../../../../../types.generated';
import { HAS_FAILING_ASSERTIONS_FILTER_NAME } from '../../../../search/utils/constants';
import { useAppConfig } from '../../../../useAppConfig';
import { useEntityData } from '../../EntityContext';
import FailingInputs from './FailingInputs';
import { DATASET_COUNT, generateQueryVariables } from './utils';

export const StyledDivider = styled(Divider)`
    margin: 16px 0;
`;

export default function UpstreamHealth() {
    const { entityData } = useEntityData();
    const [datasetsWithFailingAssertions, setDatasetsWithFailingAssertions] = useState<Dataset[]>([]);
    const [assertionsDataStart, setAssertionsDataStart] = useState(0);
    const appConfig = useAppConfig();
    const lineageEnabled: boolean = appConfig?.config?.chromeExtensionConfig?.lineageEnabled || false;

    const urn = entityData?.urn || '';

    const {
        data: assertionsData,
        loading: isLoadingAssertions,
        fetchMore: fetchMoreAssertions,
    } = useSearchAcrossLineageQuery(
        generateQueryVariables(urn, HAS_FAILING_ASSERTIONS_FILTER_NAME, assertionsDataStart, true, !lineageEnabled),
    );
    useEffect(() => {
        if (assertionsData?.searchAcrossLineage?.searchResults.length && !datasetsWithFailingAssertions.length) {
            setDatasetsWithFailingAssertions(
                assertionsData.searchAcrossLineage.searchResults.map((result) => result.entity as Dataset),
            );
        }
    }, [assertionsData?.searchAcrossLineage?.searchResults, datasetsWithFailingAssertions.length]);

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

    const hasUnhealthyUpstreams = datasetsWithFailingAssertions.length;

    if (!hasUnhealthyUpstreams) return null;

    return (
        <>
            <FailingInputs
                datasetsWithFailingAssertions={datasetsWithFailingAssertions as Dataset[]}
                totalDatasetsWithFailingAssertions={assertionsData?.searchAcrossLineage?.total || 0}
                fetchMoreAssertionsData={fetchMoreAssertionsData}
                isLoadingAssertions={isLoadingAssertions}
            />
            <StyledDivider />
        </>
    );
}
