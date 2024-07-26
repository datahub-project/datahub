import React, { useState } from 'react';
import { PlusOutlined } from '@ant-design/icons';
import { Button, Empty, Tooltip, TableProps, Table } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';

import { useGetDatasetAssertionsWithMonitorsQuery } from '../../../../../../graphql/monitor.generated';
import { useEntityData } from '../../../../../entity/shared/EntityContext';
import { DatasetAssertionsSummary } from './DatasetAssertionsSummary';
import { useIsSeparateSiblingsMode } from '../../../useIsSeparateSiblingsMode';
import { useAppConfig } from '../../../../../useAppConfig';
import { AssertionMonitorBuilderDrawer } from './assertion/builder/AssertionMonitorBuilderDrawer';
import TabToolbar from '../../../components/styled/TabToolbar';
import {
    AssertionWithMonitorDetails,
    createAssertionGroups,
    getLegacyAssertionsSummary,
    tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery,
} from './acrylUtils';
import { AssertionGroupTable } from './AssertionGroupTable';
import { updateDatasetAssertionsCache, createCachedAssertionWithMonitor } from './acrylCacheUtils';
import { useGetDatasetContractQuery } from '../../../../../../graphql/contract.generated';
import { combineEntityDataWithSiblings } from '../../../../../entity/shared/siblingUtils';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../constants';
import { AssertionRunStatus } from '@src/types.generated';
import { useBuildAssertionDescriptionLabels } from './assertion/profile/summary/utils';

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

// {getAggregationText(scope, aggregation, fields)}{' '}
// {getOperatorText(operator, parameters || undefined, nativeType || undefined)}

/**
 * Component used for rendering the Assertions Sub Tab on the Validations Tab
 */
export const AcrylAssertionList = () => {
    const [showAssertionBuilder, setShowAssertionBuilder] = useState(false);

    const { urn, entityData } = useEntityData();
    const { entityType } = useEntityData();
    const { config } = useAppConfig();
    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const { data, refetch, client, loading } = useGetDatasetAssertionsWithMonitorsQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const { data: contractData, refetch: contractRefetch } = useGetDatasetContractQuery({
        variables: { urn },
        fetchPolicy: 'cache-first',
    });

    const combinedData = isHideSiblingMode ? data : combineEntityDataWithSiblings(data);
    const assertionsWithMonitorsDetails: AssertionWithMonitorDetails[] =
        tryExtractMonitorDetailsFromAssertionsWithMonitorsQuery(combinedData) ?? [];

    const AssertionName = ({ descriptionHTML }: { descriptionHTML?: JSX.Element }) => {
        return <StyledAssertionNameContainer>{descriptionHTML}</StyledAssertionNameContainer>;
    };

    const assertionsTableCols = [
        {
            title: 'Name',
            dataIndex: '',
            key: '',
            render: (_, record: any) => {
                return <AssertionName descriptionHTML={record.descriptionHTML} />;
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

    const assertionsTableData = assertionsWithMonitorsDetails.map((assertion) => {
        const monitor =
            (assertion as any).monitor?.relationships?.length && (assertion as any).monitor?.relationships[0].entity;

        const { primaryLabel, primaryPainTextLabel } = useBuildAssertionDescriptionLabels(assertion.info, monitor);
        return {
            // for rendering need below data mapping
            type: assertion.info?.type,
            lastUpdated: assertion.info?.lastUpdated,
            tags: assertion.tags,
            descriptionHTML: primaryLabel,
            description: primaryPainTextLabel,

            // for operation need below data mapping
            urn: assertion.urn,
            platform: assertion.platform,
            lastEvaluation:
                assertion.runEvents?.runEvents?.length &&
                assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
                assertion.runEvents.runEvents[0],
            lastEvaluationTimeMs:
                assertion.runEvents?.runEvents?.length && assertion.runEvents.runEvents[0].timestampMillis,
            lastEvaluationResult:
                assertion.runEvents?.runEvents?.length &&
                assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
                assertion.runEvents.runEvents[0].result?.type,
            lastEvaluationUrl:
                assertion.runEvents?.runEvents?.length &&
                assertion.runEvents.runEvents[0].status === AssertionRunStatus.Complete &&
                assertion.runEvents.runEvents[0].result?.externalUrl,
            assertion,
            monitor:
                (assertion as any).monitor?.relationships?.length &&
                (assertion as any).monitor?.relationships[0].entity,
        };
    });

    return (
        <StyledTable
            columns={assertionsTableCols}
            dataSource={assertionsTableData}
            rowKey="urn"
            locale={{
                emptyText: <Empty description="No Assertions Found :(" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
            }}
            showHeader={false}
            pagination={false}
        />
    );
};
