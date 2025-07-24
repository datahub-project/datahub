import React, { useState } from 'react';
import styled from 'styled-components';

import { StructuredReportItem } from '@app/ingestV2/executions/components/reporting/StructuredReportItem';
import { StructuredReportLogEntry } from '@app/ingestV2/executions/components/reporting/types';
import { ShowMoreSection } from '@app/shared/ShowMoreSection';

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
    defaultActiveKey?: string;
}

export function StructuredReportItemList({ items, color, icon, pageSize = 3, defaultActiveKey }: Props) {
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
                        defaultActiveKey={defaultActiveKey}
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
