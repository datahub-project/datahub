import React, { useEffect, useState } from 'react';
import { Button, Empty, Tooltip, TableProps, Table } from 'antd';

import { useGetDatasetAssertionsWithMonitorsQuery } from '../../../../../../../graphql/monitor.generated';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '../../../../useIsSeparateSiblingsMode';
import { AssertionWithMonitorDetails, tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery } from '../acrylUtils';
import { combineEntityDataWithSiblings } from '../../../../../../entity/shared/siblingUtils';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../../constants';
import { useBuildAssertionDescriptionLabels } from '../assertion/profile/summary/utils';
import { AssertionRunStatus } from '@src/types.generated';
import { getFilteredTransformedAssertionData, transformAssertionData } from './utils';
import { AssertionListTable } from './AssertionListTable';

export type IFilter = {
    sortBy: string;
    groupBy: string;
    filterCriteria: {
        searchText: string;
        status: string[];
        type: string[];
        tags: string[];
        columns: string[];
    };
};

const dummyFilterObject: IFilter = {
    sortBy: '',
    groupBy: 'status',
    filterCriteria: {
        searchText: '',
        status: [],
        type: [],
        tags: [],
        columns: [],
    },
};

/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertionList = () => {
    const { urn } = useEntityData();

    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const [visibleAssertions, setVisibleAssertions] = useState<any>({ allAssertions: [] });
    const [allAssertionsData, setAllAssertionsData] = useState<any>({ allAssertions: [] });
    const [filter, setFilter] = useState<IFilter>({ ...dummyFilterObject });
    const [assertionMonitorData, setAssertionMonitorData] = useState<any[]>([]);
    const { data } = useGetDatasetAssertionsWithMonitorsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
        const assertionsWithMonitorsDetails: AssertionWithMonitorDetails[] =
            tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(combinedData) ?? [];
        setAssertionMonitorData(assertionsWithMonitorsDetails);
        const transformedAssertions = transformAssertionData(assertionsWithMonitorsDetails);
        setVisibleAssertions(transformedAssertions);
        setAllAssertionsData(transformedAssertions);
        setFilter({ ...dummyFilterObject });
    }, [data]);

    useEffect(() => {
        const filteredAssertionData = getFilteredTransformedAssertionData(assertionMonitorData, filter);
        setVisibleAssertions(filteredAssertionData);
    }, [filter]);

    return <AssertionListTable dataSource={visibleAssertions.allAssertions || []} />;
};
