/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { StructuredReportLogEntry } from '@app/ingestV2/executions/components/reporting/types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-left: 12px;
`;

const Title = styled.div`
    font-size: 14px;
    font-weight: bold;
`;

const Item = styled.pre`
    padding: 6px;
    font-size: 12px;
    border-radius: 2px;
    background-color: ${ANTD_GRAY[3]};
    color: ${ANTD_GRAY[8]};
`;

interface Props {
    item: StructuredReportLogEntry;
}

export function StructuredReportItemContext({ item }: Props) {
    return (
        <Container>
            <Tooltip showArrow={false} title="Additional context about the source of the issue" placement="left">
                <Title>Context</Title>
            </Tooltip>
            {item.context.length
                ? // eslint-disable-next-line react/no-array-index-key
                  item.context.map((contextItem, index) => <Item key={`${contextItem}-${index}`}>{contextItem}</Item>)
                : 'No additional context found.'}
        </Container>
    );
}
