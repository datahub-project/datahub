import dayjs from 'dayjs';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import {
    AnyOperationType,
    CustomOperationType,
    Operation,
    OperationsData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { convertAggregationsToOperationsData } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/utils';
import { Button, Text } from '@src/alchemy-components';
import { DayData } from '@src/alchemy-components/components/CalendarChart/types';
import { abbreviateNumber } from '@src/app/dataviz/utils';
import { formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';
import { pluralize, pluralizeIfIrregular } from '@src/app/shared/textUtil';
import { OperationType } from '@src/types.generated';

const Container = styled.div`
    display: flex;
    flex-direction: column;
`;

const RowContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    height: 100%;
`;

const ColumnContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const ValueContainer = styled(RowContainer)`
    align-items: center;
    gap: 4px;
`;

const LinkContainer = styled(RowContainer)`
    align-items: end;
    position: relative;
    height: auto;
`;

const Square = styled.div<{ $color: string }>`
    width: 12px;
    height: 12px;
    border-radius: 2px;
    background: ${(props) => props.$color};
`;

type ChangeHistoryPopoverProps = {
    datum: DayData<OperationsData>;
    onViewDetails?: () => void;
    hasData?: boolean;
    colorAccessors: { [key: string]: (value?: OperationsData) => string };
    defaultCustomOperationTypes?: CustomOperationType[];
    selectedOperationTypes: AnyOperationType[];
};

export default function ChangeHistoryPopover({
    datum,
    onViewDetails,
    hasData,
    colorAccessors,
    defaultCustomOperationTypes,
    selectedOperationTypes,
}: ChangeHistoryPopoverProps) {
    const operations = useMemo(
        () =>
            Object.entries(
                (datum.value?.operations ??
                    convertAggregationsToOperationsData({}, defaultCustomOperationTypes)?.operations) ||
                    {},
            )
                .map(([_, value]) => value)
                .filter((value) => selectedOperationTypes.includes(value.key))
                .filter((value) => value.value > 0)
                // order from most changes to least
                .sort((a, b) => b.value - a.value),
        [datum.value?.operations, selectedOperationTypes, defaultCustomOperationTypes],
    );

    const totalAmoutOfOperations = useMemo(() => operations.reduce((sum, value) => sum + value.value, 0), [operations]);

    const renderTotalRow = (value: number) => {
        return (
            <Text size="sm" color="gray" weight="bold" type="div" data-testid="total-changes">
                {abbreviateNumber(value)} {pluralize(value, 'Change')}
            </Text>
        );
    };

    const renderNoData = () => {
        return (
            <Text size="sm" color="gray" weight="bold" data-testid="no-data-reported">
                No data reported
            </Text>
        );
    };

    const renderNoDataThisDay = () => {
        return (
            <Text size="sm" color="gray" weight="bold" data-testid="no-changes-this-day">
                No changes this day
            </Text>
        );
    };

    const renderOperation = (operation: Operation) => {
        const color = colorAccessors?.[operation.key](datum.value);
        const name = operation.type === OperationType.Custom ? operation.name : pluralizeIfIrregular(operation.name);

        return (
            <ValueContainer key={operation.key} data-testid={`operation-${operation.key}`}>
                <Square $color={color} />
                <Text size="sm" color="gray" data-testid="operation-name">
                    {name}
                </Text>
                <Text size="sm" color="gray" weight="bold" data-testid="operation-value">
                    {formatNumberWithoutAbbreviation(operation.value)}
                </Text>
            </ValueContainer>
        );
    };

    const renderChanges = () => {
        if (!hasData) return renderNoData();
        if (totalAmoutOfOperations === 0) return renderNoDataThisDay();
        return (
            <>
                {renderTotalRow(totalAmoutOfOperations)}
                <RowContainer>
                    <ColumnContainer>{operations.map((value) => renderOperation(value))}</ColumnContainer>
                    <LinkContainer>
                        {operations.length > 0 && (
                            <Button variant="text" size="xs" onClick={() => onViewDetails?.()}>
                                View Details
                            </Button>
                        )}
                    </LinkContainer>
                </RowContainer>
            </>
        );
    };

    return (
        <Container data-testid={`day-popover-${datum.key}`}>
            <Text size="sm" color="gray" type="div">
                {dayjs(datum.day).format('dddd, MMM DD ’YY')}
            </Text>
            {renderChanges()}
        </Container>
    );
}
