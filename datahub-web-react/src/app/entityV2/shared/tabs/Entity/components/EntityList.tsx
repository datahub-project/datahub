import { Pagination, Text } from '@components';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { PreviewType } from '@app/entityV2/Entity';
import { Message } from '@app/shared/Message';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { SearchCfg } from '@src/conf';

import { EntityType } from '@types';

const LOADING_MARGIN_TOP = '10%';

const ScrollWrapper = styled.div`
    overflow: auto;
    height: 100%;

    &::-webkit-scrollbar {
        height: 12px;
        width: 5px;
        background: ${(props) => props.theme.colors.scrollbarTrack};
    }
    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: ${(props) => props.theme.colors.shadowXs};
    }
`;

const StyledList = styled.div`
    padding-left: 40px;
    padding-right: 40px;
`;

const ListHeader = styled(Text)`
    margin-left: -20px;
`;

const StyledListItem = styled.div`
    padding-top: 20px;
`;

const PaginationInfoContainer = styled.span`
    padding: 8px;
    padding-left: 16px;
    border-top: 1px solid;
    border-color: ${(props) => props.theme.colors.border};
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

const PaginationInfo = styled(Text)`
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
    const { t } = useTranslation('entity.profile.tabs');
    const { t: tc } = useTranslation('common.feedback');

    return (
        <>
            <ScrollWrapper>
                <StyledList>
                    <ListHeader weight="semiBold">
                        {title || `${entities.length || 0} ${entityRegistry.getCollectionName(type)}`}
                    </ListHeader>
                    {entities.map((item) => (
                        <StyledListItem key={item.urn}>
                            {entityRegistry.renderPreview(type, PreviewType.PREVIEW, item)}
                        </StyledListItem>
                    ))}
                </StyledList>
            </ScrollWrapper>
            {loading && <Message type="loading" content={tc('loading')} style={{ marginTop: LOADING_MARGIN_TOP }} />}
            {error && <Message type="error" content={t('entity.list.loadError')} />}
            {showPagination && (
                <PaginationInfoContainer>
                    <PaginationInfo>
                        <Trans
                            t={t}
                            i18nKey="entity.paginationRange"
                            values={{
                                start: lastResultIndex > 0 ? ((page as number) - 1) * pageSize + 1 : 0,
                                end: lastResultIndex,
                                total: totalAssets,
                            }}
                            components={{ bold: <b /> }}
                        />
                    </PaginationInfo>
                    <StyledPagination
                        currentPage={page ?? 1}
                        itemsPerPage={pageSize}
                        total={totalAssets ?? 0}
                        showLessItems
                        onPageChange={onChangePage}
                        showSizeChanger={(totalAssets ?? 0) > SearchCfg.RESULTS_PER_PAGE}
                        onShowSizeChange={(_currNum, newNum) => setNumResultsPerPage?.(newNum)}
                        pageSizeOptions={['10', '20', '50', '100']}
                    />
                </PaginationInfoContainer>
            )}
        </>
    );
};
