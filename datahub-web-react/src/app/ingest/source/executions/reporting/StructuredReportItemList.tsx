import React, { useState } from 'react';
import styled from 'styled-components';
import { StructuredReportItem as StructuredReportItemType } from '../../types';
import { StructuredReportItem } from './StructuredReportItem';
import { ShowMoreSection } from '../../../../shared/ShowMoreSection';

const ItemList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

interface Props {
    items: StructuredReportItemType[];
    color: string;
    icon?: React.ComponentType<any>;
    pageSize?: number;
}

export function StructuredReportItemList({ items, color, icon, pageSize = 3 }: Props) {
    const [visibleCount, setVisibleCount] = useState(pageSize);
    const visibleItems = items.slice(0, visibleCount);
    const totalCount = items.length;

    return (
        <>
            <ItemList>
                {visibleItems.map((item) => (
                    <StructuredReportItem item={item} color={color} icon={icon} key={item.rawType} />
                ))}
            </ItemList>
            {totalCount > visibleCount ? (
                <ShowMoreSection
                    totalCount={totalCount}
                    visibleCount={visibleCount}
                    setVisibleCount={setVisibleCount}
                    pageSize={pageSize}
                />
            ) : null}
        </>
    );
}
