import React from 'react';
import { List, Pagination, Typography } from 'antd';
import styled from 'styled-components';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { PreviewType } from '../../../../Entity';
import { EntityType } from '../../../../../../types.generated';
import { SearchCfg } from '../../../../../../conf';
import { Message } from '../../../../../shared/Message';

const ScrollWrapper = styled.div`
    overflow: auto;
    height: 100%;

    &::-webkit-scrollbar {
        height: 12px;
        width: 5px;
        background: #f2f2f2;
    }
    &::-webkit-scrollbar-thumb {
        background: #cccccc;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
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
    padding: 12px 12px 12px 12px;
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const PaginationInfo = styled(Typography.Text)`
    padding: 0px;
    width: 20%;
`;

type EntityListProps = {
    type: EntityType;
    entities: Array<any>;
    title?: string;
    totalAssets?: number;
    pageSize?: any;
    page?: number;
    lastResultIndex?: any;
    showPagination?: boolean;
    loading?: boolean;
    error?: any;
    onChangePage?: (number: any) => void;
    setNumResultsPerPage?: (number: any) => void;
};

export const EntityList = ({
    type,
    entities,
    title,
    totalAssets,
    pageSize,
    page,
    lastResultIndex,
    showPagination = false,
    loading = false,
    error = undefined,
    onChangePage,
    setNumResultsPerPage,
}: EntityListProps) => {
    const entityRegistry = useEntityRegistry();

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
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            {error && <Message type="error" content="Failed to load results! An unexpected error occurred." />}
            {showPagination && (
                <PaginationInfoContainer>
                    <PaginationInfo>
                        <b>
                            {lastResultIndex > 0 ? ((page as number) - 1) * pageSize + 1 : 0} - {lastResultIndex}
                        </b>{' '}
                        of <b>{totalAssets}</b>
                    </PaginationInfo>
                    <StyledPagination
                        current={page}
                        pageSize={pageSize}
                        total={totalAssets}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={(totalAssets as any) > SearchCfg.RESULTS_PER_PAGE}
                        onShowSizeChange={(_currNum, newNum) => setNumResultsPerPage?.(newNum)}
                        pageSizeOptions={['10', '20', '50', '100']}
                    />
                </PaginationInfoContainer>
            )}
        </>
    );
};
