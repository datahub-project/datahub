import React, { useEffect, useState } from 'react';
import { List, Pagination, Typography } from 'antd';
import styled from 'styled-components';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { PreviewType } from '../../../../Entity';
import { EntityType } from '../../../../../../types.generated';
import { SearchCfg } from '../../../../../../conf';
import { useTaskPagination } from './TaskPaginationContext';

const ScrollWrapper = styled.div`
    overflow: auto;
    max-height: 100%;
`;

const StyledList = styled(List)`
    padding-left: 40px;
    padding-right: 40px;
    .ant-list-items > .ant-list-item {
        padding-right: 0px;
        padding-left: 0px;
    }
    > .ant-list-header {
        padding-right: 0px;
        padding-left: 0px;
        font-size: 14px;
        font-weight: 600;
        margin-left: -20px;
        border-bottom: none;
        padding-bottom: 0px;
        padding-top: 15px;
    }
` as typeof List;

const StyledListItem = styled(List.Item)`
    padding-top: 20px;
`;

const PaginationInfoContainer = styled.span`
    padding: 8px;
    padding-left: 16px;
    border-top: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const StyledPagination = styled(Pagination)`
    margin: 0px;
    padding: 0px;
`;

const PaginationInfo = styled(Typography.Text)`
    padding: 0px;
`;

type EntityListProps = {
    type: EntityType;
    entities: Array<any>;
    title?: string;
    totalJobs?: number | null;
    pageSize?: any;
    lastResultIndex?: any;
    showTaskPagination?: boolean;
};

export const EntityList = ({
    type,
    entities,
    title,
    totalJobs,
    pageSize,
    lastResultIndex,
    showTaskPagination = false,
}: EntityListProps) => {
    const entityRegistry = useEntityRegistry();
    const { updateData } = useTaskPagination();

    const [page, setPage] = useState(1);
    const [numResultsPerPage, setNumResultsPerPage] = useState(SearchCfg.RESULTS_PER_PAGE);

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    useEffect(() => {
        updateData(numResultsPerPage, (page - 1) * numResultsPerPage);
    }, [page, numResultsPerPage, updateData]);

    return (
        <>
            <ScrollWrapper>
                <StyledList
                    bordered
                    dataSource={entities}
                    header={title || `${entities.length || 0} ${entityRegistry.getCollectionName(type)}`}
                    renderItem={(item) => (
                        <StyledListItem>{entityRegistry.renderPreview(type, PreviewType.PREVIEW, item)}</StyledListItem>
                    )}
                />
            </ScrollWrapper>
            {showTaskPagination && (
                <PaginationInfoContainer>
                    <PaginationInfo>
                        <b>
                            {lastResultIndex > 0 ? (page - 1) * pageSize + 1 : 0} - {lastResultIndex}
                        </b>{' '}
                        of <b>{totalJobs}</b>
                    </PaginationInfo>
                    <StyledPagination
                        current={page}
                        pageSize={numResultsPerPage}
                        total={totalJobs as any}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={(totalJobs as any) > SearchCfg.RESULTS_PER_PAGE}
                        onShowSizeChange={(_currNum, newNum) => setNumResultsPerPage(newNum)}
                        pageSizeOptions={['10', '20', '50', '100']}
                    />
                </PaginationInfoContainer>
            )}
        </>
    );
};
