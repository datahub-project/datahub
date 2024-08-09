import React, { useState } from 'react';
import styled from 'styled-components';
import { StructuredReportItem } from './StructuredReportItem';
import { ShowMoreSection } from '../../../../shared/ShowMoreSection';
import { StructuredReportLogEntry } from '../../types';

const ItemList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

interface Props {
    items: StructuredReportLogEntry[];
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
                    <StructuredReportItem
                        item={item}
                        color={color}
                        icon={icon}
                        key={`${item.message}-${item.context}`}
                    />
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
