/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';
import styled from 'styled-components';

import {
    AggregationGroup,
    Operation,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { Pill } from '@src/alchemy-components';
import { ColorOptions } from '@src/alchemy-components/theme/config';
import { abbreviateNumber } from '@src/app/dataviz/utils';
import { pluralize } from '@src/app/shared/textUtil';

const GROUP_TO_PILL_COLOR_MAPPING = new Map<string, ColorOptions>([
    [AggregationGroup.Purple, 'violet'],
    [AggregationGroup.Red, 'red'],
]);

const Container = styled.div`
    max-height: 24px;
`;

type ChangeTypeSummaryPillProps = {
    operation: Operation;
    onClick?: () => void;
    selected?: boolean;
};

export default function ChangeTypeSummaryPill({ operation, onClick, selected }: ChangeTypeSummaryPillProps) {
    const label = useMemo(() => {
        return `${abbreviateNumber(operation.value)} ${pluralize(operation.value, operation.name)}`;
    }, [operation]);

    const colorScheme = useMemo(
        () => (selected ? GROUP_TO_PILL_COLOR_MAPPING.get(operation.group) : 'gray'),
        [operation, selected],
    );

    return (
        <Container>
            <Pill
                size="sm"
                label={label}
                color={colorScheme}
                clickable={!!onClick}
                onPillClick={onClick}
                dataTestId={`summary-pill-${operation.key}`}
            />
        </Container>
    );
}
