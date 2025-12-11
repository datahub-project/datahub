/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useState } from 'react';
import styled from 'styled-components';

import { StructuredReportItem } from '@app/ingest/source/executions/reporting/StructuredReportItem';
import { StructuredReportLogEntry } from '@app/ingest/source/types';
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
