import React, { useState } from 'react';
import { Typography, Table, Popover, TablePaginationConfig } from 'antd';
import { TooltipPlacement } from 'antd/es/tooltip';
import { InfoCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { QueriesTabSection, Query } from './types';
import { DEFAULT_PAGE_SIZE } from './utils/constants';
import { ANTD_GRAY, ANTD_GRAY_V2 } from '../../../constants';
import useQueryTableColumns from './useQueryTableColumns';

const QueriesTitleSection = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 20px;
`;

const QueriesTitle = styled(Typography.Title)`
    && {
        margin: 0px;
    }
`;

const StyledInfoOutlined = styled(InfoCircleOutlined)`
    margin-left: 8px;
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

const StyledTable = styled(Table)`
    .ant-table-thead > tr > th {
        font-weight: 700;
        font-size: 14px;
        line-height: 16px;
        color: ${ANTD_GRAY_V2[12]};
    }
`;

type Props = {
    title: string;
    queries: Query[];
    tooltip?: string;
    tooltipPosition?: TooltipPlacement;
    initialPageSize?: number;
    showDetails?: boolean;
    showEdit?: boolean;
    showDelete?: boolean;
    onDeleted?: (query) => void;
    onEdited?: (query) => void;
    section: QueriesTabSection;
};

export default function QueriesListSection({
    title,
    tooltip,
    tooltipPosition,
    queries,
    initialPageSize = DEFAULT_PAGE_SIZE,
    showDetails,
    showEdit,
    showDelete,
    onDeleted,
    onEdited,
    section,
}: Props) {
    /**
     * Table state
     */
    const [hoveredQueryUrn, setHoveredQueryUrn] = useState<string | null>(null);

    const {
        titleColumn,
        descriptionColumn,
        queryTextColumn,
        createdByColumn,
        createdDateColumn,
        powersColumn,
        usedByColumn,
        popularityColumn,
        lastRunColumn,
        editColumn,
    } = useQueryTableColumns({
        queries,
        hoveredQueryUrn,
        showDelete,
        showDetails,
        showEdit,
        onDeleted,
        onEdited,
    });

    const highlightedQueriesColumns = [
        titleColumn,
        descriptionColumn,
        queryTextColumn(),
        createdByColumn,
        createdDateColumn,
        editColumn,
    ];

    const popularQueriesColumns = [queryTextColumn('60%'), usedByColumn, lastRunColumn, popularityColumn];

    const downstreamQueriesColumns = [queryTextColumn('60%'), powersColumn, lastRunColumn];

    const recentQueriesColumns = [queryTextColumn('60%'), lastRunColumn];

    const showPagination = queries.length > initialPageSize;
    const pagionationOptions = showPagination
        ? ({ defaultPageSize: initialPageSize, position: ['bottomCenter'] } as TablePaginationConfig)
        : false;

    return (
        <div>
            <QueriesTitleSection>
                <QueriesTitle level={4}>{title}</QueriesTitle>
                {tooltip && (
                    <Popover content={tooltip} placement={tooltipPosition}>
                        <StyledInfoOutlined />
                    </Popover>
                )}
            </QueriesTitleSection>
            {section === QueriesTabSection.Highlighted && (
                <StyledTable
                    dataSource={queries}
                    columns={highlightedQueriesColumns}
                    pagination={pagionationOptions}
                    scroll={{ y: 400 }}
                    onRow={(row) => {
                        return {
                            onMouseEnter: () => setHoveredQueryUrn((row as Query).urn || ''),
                            onMouseLeave: () => setHoveredQueryUrn(null),
                        };
                    }}
                />
            )}
            {section === QueriesTabSection.Popular && (
                <StyledTable
                    dataSource={queries}
                    columns={popularQueriesColumns}
                    pagination={pagionationOptions}
                    scroll={{ y: 400 }}
                />
            )}
            {section === QueriesTabSection.Downstream && (
                <StyledTable
                    dataSource={queries}
                    columns={downstreamQueriesColumns}
                    pagination={pagionationOptions}
                    scroll={{ y: 400 }}
                />
            )}
            {section === QueriesTabSection.Recent && (
                <StyledTable
                    dataSource={queries}
                    columns={recentQueriesColumns}
                    pagination={pagionationOptions}
                    scroll={{ y: 400 }}
                />
            )}
        </div>
    );
}
