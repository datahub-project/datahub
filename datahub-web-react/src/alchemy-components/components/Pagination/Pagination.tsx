import { CaretLeft } from '@phosphor-icons/react/dist/csr/CaretLeft';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Pagination as PaginationComponent } from 'antd';
import type { PaginationProps as AntdPaginationProps } from 'antd';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { PaginationContainer } from '@components/components/Pagination/components';
import { PaginationProps, paginationDefaults } from '@components/components/Pagination/types';

export const Pagination = ({
    currentPage = paginationDefaults.currentPage,
    itemsPerPage = paginationDefaults.itemsPerPage,
    total = paginationDefaults.total,
    loading = paginationDefaults.loading,
    onPageChange,
    className,
    itemRender,
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
            />
        </PaginationContainer>
    );
};
