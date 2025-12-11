/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { StructuredReportItem } from '@app/ingestV2/executions/components/reporting/StructuredReportItem';
import { StructuredReportLogEntry } from '@app/ingestV2/executions/components/reporting/types';

const ItemList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

interface Props {
    items: StructuredReportLogEntry[];
    color: string;
    textColor?: string;
    icon?: string;
    defaultActiveKey?: string;
}

export function StructuredReportItemList({ items, color, textColor, icon, defaultActiveKey }: Props) {
    return (
        <ItemList>
            {items.map((item) => (
                <StructuredReportItem
                    item={item}
                    color={color}
                    textColor={textColor}
                    icon={icon}
                    defaultActiveKey={defaultActiveKey}
                    key={`${item.message}-${item.context}`}
                />
            ))}
        </ItemList>
    );
}
