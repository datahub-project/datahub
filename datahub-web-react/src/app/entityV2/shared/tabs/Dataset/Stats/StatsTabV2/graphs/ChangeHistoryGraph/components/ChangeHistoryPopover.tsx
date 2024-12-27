import React from 'react';
import dayjs from 'dayjs';
import { Button, Text } from '@src/alchemy-components';
import styled from 'styled-components';
import { pluralize } from '@src/app/shared/textUtil';
import { DayData } from '@src/alchemy-components/components/CalendarChart/types';
import { ValueType } from '../hooks/useChangeHistoryData';

const Container = styled.div`
    display: flex;
    flex-direction: column;
`;

const RowContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
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
    // FIY: show link at the bottom side.
    // "align-item: end;" doesn't work here
    padding-top: 39px;
    position: relative;
    height: 100%;
`;

const Square = styled.div<{ $color: string }>`
    width: 12px;
    height: 12px;
    border-radius: 2px;
    background: ${(props) => props.$color};
`;

type ChangeHistoryPopoverProps = {
    datum: DayData<ValueType>;
    onViewDetails?: () => void;
    hasData?: boolean;
    insertsColorAccessor: (value?: ValueType) => string;
    updatesColorAccessor: (value?: ValueType) => string;
    deletesColorAccessor: (value?: ValueType) => string;
};

export default function ChangeHistoryPopover({
    datum,
    onViewDetails,
    hasData,
    insertsColorAccessor,
    updatesColorAccessor,
    deletesColorAccessor,
}: ChangeHistoryPopoverProps) {
    const total = datum?.value?.total ?? 0;

    const renderTotalRow = (value: number) => {
        return (
            <Text size="sm" color="gray" weight="bold" type="div">
                {value} {pluralize(value, 'Change')}
            </Text>
        );
    };

    const renderNoData = () => {
        return (
            <Text size="sm" color="gray" weight="bold">
                No data reported
            </Text>
        );
    };

    const renderValue = (value: number, changeType: string, color: string) => {
        return (
            <ValueContainer>
                <Square $color={color} />
                <Text size="sm" color="gray">
                    {changeType}
                </Text>
                <Text size="sm" color="gray" weight="bold">
                    {value}
                </Text>
            </ValueContainer>
        );
    };

    return (
        <Container>
            <Text size="sm" color="gray" type="div">
                {dayjs(datum.day).format('dddd, MMM DD ’YY')}
            </Text>
            {!hasData ? (
                renderNoData()
            ) : (
                <>
                    {renderTotalRow(total)}
                    <RowContainer>
                        <ColumnContainer>
                            {renderValue(datum.value?.updates ?? 0, 'Updates', updatesColorAccessor(datum.value))}
                            {renderValue(datum.value?.inserts ?? 0, 'Inserts', insertsColorAccessor(datum.value))}
                            {renderValue(datum.value?.deletes ?? 0, 'Deletes', deletesColorAccessor(datum.value))}
                        </ColumnContainer>
                        <LinkContainer>
                            <Button variant="text" size="xs" onClick={onViewDetails}>
                                View Details
                            </Button>
                        </LinkContainer>
                    </RowContainer>
                </>
            )}
        </Container>
    );
}
