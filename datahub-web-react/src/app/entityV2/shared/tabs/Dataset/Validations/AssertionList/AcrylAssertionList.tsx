import { TableLoadingSkeleton } from '@app/entityV2/shared/TableLoadingSkeleton';
import React, { useEffect, useState } from 'react';
import { Empty } from 'antd';
import { useGetDatasetContractQuery } from '@src/graphql/contract.generated';
import { Assertion, DataContract } from '@src/types.generated';
import { useGetDatasetAssertionsWithRunEventsQuery } from '@src/graphql/dataset.generated';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '../../../../useIsSeparateSiblingsMode';
import { tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery } from '../acrylUtils';
import { combineEntityDataWithSiblings } from '../../../../../../entity/shared/siblingUtils';
import { getFilteredTransformedAssertionData } from './utils';
import { AssertionTable, AssertionListFilter } from './types';
import { AssertionListTitleContainer } from './AssertionListTitleContainer';
import { AcrylAssertionListFilters } from './AcrylAssertionListFilters';
import { AcrylAssertionListTable } from './AcrylAssertionListTable';
import { ASSERTION_DEFAULT_FILTERS, ASSERTION_DEFAULT_RAW_DATA } from './constant';

/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertionList = () => {
    const { urn } = useEntityData();

    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const [visibleAssertions, setVisibleAssertions] = useState<AssertionTable>({
        ...ASSERTION_DEFAULT_RAW_DATA,
    });
    // TODO we need to create setter function to set the filter as per the filter component
    const [filter, setFilters] = useState<AssertionListFilter>(ASSERTION_DEFAULT_FILTERS);

    const [assertionMonitorData, setAssertionMonitorData] = useState<Assertion[]>([]);

    const { data, refetch, loading } = useGetDatasetAssertionsWithRunEventsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });
    const { data: contractData, refetch: contractRefetch } = useGetDatasetContractQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const contract: DataContract = contractData?.dataset?.contract as DataContract;

    // get filtered Assertion as per the filter object
    const getFilteredAssertions = (assertions: Assertion[]) => {
        const filteredAssertionData: AssertionTable = getFilteredTransformedAssertionData(assertions, filter);
        setVisibleAssertions(filteredAssertionData);
    };

    useEffect(() => {
        const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
        const assertionsWithMonitorsDetails: Assertion[] =
            tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(combinedData) ?? [];
        setAssertionMonitorData(assertionsWithMonitorsDetails);
        getFilteredAssertions(assertionsWithMonitorsDetails);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data]);

    useEffect(() => {
        // after filter change need to get filtered assertions
        if (assertionMonitorData?.length > 0) {
            getFilteredAssertions(assertionMonitorData);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [filter]);

    const renderListTable = () => {
        if (loading) {
            return <TableLoadingSkeleton />;
        }
        if ((visibleAssertions?.assertions || []).length > 0) {
            return (
                <AcrylAssertionListTable
                    contract={contract}
                    assertionData={visibleAssertions}
                    filter={filter}
                    refetch={() => {
                        refetch();
                        contractRefetch();
                    }}
                />
            );
        }
        return <Empty description="No assertions have run" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
    };

    return (
        <>
            <AssertionListTitleContainer />
            {assertionMonitorData?.length > 0 && (
                <AcrylAssertionListFilters
                    filterOptions={visibleAssertions?.filterOptions}
                    originalFilterOptions={visibleAssertions?.originalFilterOptions}
                    setFilters={setFilters}
                    filter={filter}
                    filteredAssertions={visibleAssertions}
                />
            )}

            {renderListTable()}
        </>
    );
};
