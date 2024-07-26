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
import { transformAssertionData } from './utils';

export const StyledTable = styled(Table)`
    max-width: none;
    overflow: inherit;
    height: inherit;
    &&& .ant-table-thead .ant-table-cell {
        font-weight: 600;
        font-size: 12px;
        color: ${ANTD_GRAY[8]};
    }
    &&
        .ant-table-thead
        > tr
        > th:not(:last-child):not(.ant-table-selection-column):not(.ant-table-row-expand-icon-cell):not(
            [colspan]
        )::before {
        border: 1px solid ${ANTD_GRAY[4]};
    }
    &&& .ant-table-cell {
        background-color: transparent;
    }
    &&& .acryl-assertions-table-row {
        cursor: pointer;
        background-color: ${ANTD_GRAY[2]};
        :hover {
            background-color: ${ANTD_GRAY[3]};
        }
    }
    &&& .acryl-selected-assertions-table-row {
        background-color: ${ANTD_GRAY[4]};
    }
`;
const StyledAssertionNameContainer = styled.div`
    display: flex;
`;

/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertionList = () => {
    const { urn } = useEntityData();

    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const [visibleAssertions, setVisibleAssertions] = useState<any[]>([]);
    const [allAssertions, setAllAssertions] = useState<any[]>([]);

    const { data } = useGetDatasetAssertionsWithMonitorsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
        const assertionsWithMonitorsDetails: AssertionWithMonitorDetails[] =
            tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(combinedData) ?? [];
        const transformedAssertions = transformAssertionData(assertionsWithMonitorsDetails);
        setVisibleAssertions(transformedAssertions);
        setAllAssertions(transformedAssertions);
    }, [data]);

    const AssertionName = ({ record }: any) => {
        const { primaryLabel } = useBuildAssertionDescriptionLabels(record.assertion.info, record.monitor);
        return <StyledAssertionNameContainer>{primaryLabel}</StyledAssertionNameContainer>;
    };

    const assertionsTableCols = [
        {
            title: 'Name',
            dataIndex: '',
            key: '',
            render: (_, record: any) => {
                return <AssertionName record={record} />;
            },
            width: '50%',
        },
        {
            title: 'Last Updated',
            dataIndex: '',
            key: '',
            render: (_, record: any) => {
                return <div>Last Updated</div>;
            },
            width: '17%',
        },
        {
            title: 'Tags',
            dataIndex: '',
            key: 'tags',
            width: '17%',
            render: (tags?: string[]) => {
                return <div> Tags {tags?.toString()} </div>;
            },
        },
        {
            title: '',
            dataIndex: '',
            key: '',
            width: '16%',
            render: () => {
                return <div> Actions </div>;
            },
        },
    ];

    return (
        <StyledTable
            columns={assertionsTableCols}
            dataSource={visibleAssertions}
            rowKey="urn"
            locale={{
                emptyText: <Empty description="No Assertions Found :(" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
            }}
            showHeader={false}
            pagination={false}
        />
    );
};
