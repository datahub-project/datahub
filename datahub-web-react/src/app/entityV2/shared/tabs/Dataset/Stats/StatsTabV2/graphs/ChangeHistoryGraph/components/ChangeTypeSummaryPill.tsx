import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import {
    AggregationGroup,
    Operation,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { Pill } from '@src/alchemy-components';
import { ColorOptions } from '@src/alchemy-components/theme/config';
import { abbreviateNumber } from '@src/app/dataviz/utils';

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
    const { t } = useTranslation('entity.profile.stats');
    const label = useMemo(
        () =>
            t('changeTypeSummaryPill.operationCount', {
                count: operation.value,
                formattedCount: abbreviateNumber(operation.value),
                name: operation.name,
            }),
        [operation, t],
    );

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
