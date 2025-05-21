import { Empty } from 'antd';
import React, { useEffect, useState } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { combineEntityDataWithSiblings } from '@app/entity/shared/siblingUtils';
import { TableLoadingSkeleton } from '@app/entityV2/shared/TableLoadingSkeleton';
import { AcrylAssertionListFilters } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListFilters';
import { AcrylAssertionListTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionListTable';
import { AssertionListTitleContainer } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AssertionListTitleContainer';
import {
    ASSERTION_DEFAULT_FILTERS,
    ASSERTION_DEFAULT_RAW_DATA,
} from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/constant';
import { AssertionListFilter, AssertionTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { getFilteredTransformedAssertionData } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';
import { tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { useGetDatasetContractQuery } from '@src/graphql/contract.generated';
import { useGetDatasetAssertionsWithRunEventsQuery } from '@src/graphql/dataset.generated';
import { Assertion, DataContract } from '@src/types.generated';

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
    const [selectedFilters, setSelectedFilters] = useState<AssertionListFilter>(ASSERTION_DEFAULT_FILTERS);

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
        const filteredAssertionData: AssertionTable = getFilteredTransformedAssertionData(assertions, selectedFilters);
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
    }, [selectedFilters]);

    const handleFilterChange = (filter: any) => {
        setSelectedFilters(filter);
    };

    const renderListTable = () => {
        if (loading) {
            return <TableLoadingSkeleton />;
        }
        if ((visibleAssertions?.assertions || []).length > 0) {
            return (
                <AcrylAssertionListTable
                    contract={contract}
                    assertionData={visibleAssertions}
                    filter={selectedFilters}
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
                    filteredAssertions={visibleAssertions}
                    selectedFilters={selectedFilters}
                    setSelectedFilters={setSelectedFilters}
                    handleFilterChange={handleFilterChange}
                />
            )}
            {renderListTable()}
        </>
    );
};
