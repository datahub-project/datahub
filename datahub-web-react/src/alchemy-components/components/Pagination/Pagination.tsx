import { CaretLeft } from '@phosphor-icons/react/dist/csr/CaretLeft';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Pagination as PaginationComponent } from 'antd';
import type { PaginationProps as AntdPaginationProps } from 'antd';
import React, { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { PaginationContainer } from '@components/components/Pagination/components';
import { PaginationProps, paginationDefaults } from '@components/components/Pagination/types';
import { SimpleSelect } from '@components/components/Select';

const DEFAULT_PAGE_SIZE_OPTIONS = [10, 20, 50, 100];

export const Pagination = ({
    currentPage = paginationDefaults.currentPage,
    itemsPerPage = paginationDefaults.itemsPerPage,
    total = paginationDefaults.total,
    loading = paginationDefaults.loading,
    onPageChange,
    className,
    itemRender,
    showSizeChanger,
    pageSizeOptions,
    onShowSizeChange,
    ...props
}: PaginationProps) => {
    const { t } = useTranslation('alchemy');

    const defaultItemRender = useMemo<AntdPaginationProps['itemRender']>(
        () => (_page, type, originalElement) => {
            if (type === 'prev') {
                return (
                    <button
                        type="button"
                        className="ant-pagination-item-link"
                        aria-label={t('pagination.previousPage')}
                    >
                        <CaretLeft />
                    </button>
                );
            }
            if (type === 'next') {
                return (
                    <button type="button" className="ant-pagination-item-link" aria-label={t('pagination.nextPage')}>
                        <CaretRight />
                    </button>
                );
            }
            return originalElement;
        },
        [t],
    );

    const pageSizeSelectOptions = useMemo(
        () =>
            (pageSizeOptions ?? DEFAULT_PAGE_SIZE_OPTIONS).map((option) => ({
                value: `${option}`,
                label: t('pagination.pageSize', { size: option }),
            })),
        [pageSizeOptions, t],
    );

    // Mirrors antd's own size-changer contract: it clamps the current page to the new page count,
    // then notifies onShowSizeChange before onChange. Consumers rely on one or the other — some
    // only read the page size off onPageChange's second argument.
    const onPageSizeChange = useCallback(
        (values: string[]) => {
            const nextPageSize = Number(values[0]);
            if (!Number.isFinite(nextPageSize) || nextPageSize <= 0) return;

            const nextPage = Math.min(currentPage, Math.max(Math.ceil(total / nextPageSize), 1));
            onShowSizeChange?.(nextPage, nextPageSize);
            onPageChange?.(nextPage, nextPageSize);
        },
        [currentPage, total, onShowSizeChange, onPageChange],
    );

    if (loading) {
        return null;
    }
    return (
        <PaginationContainer className={className}>
            <PaginationComponent
                {...props}
                current={currentPage}
                pageSize={itemsPerPage}
                total={total}
                onChange={onPageChange}
                itemRender={itemRender ?? defaultItemRender}
                showSizeChanger={false}
            />
            {showSizeChanger && (
                <SimpleSelect
                    options={pageSizeSelectOptions}
                    values={[`${itemsPerPage}`]}
                    onUpdate={onPageSizeChange}
                    showClear={false}
                    width="fit-content"
                    dataTestId="pagination-page-size-select"
                />
            )}
        </PaginationContainer>
    );
};
