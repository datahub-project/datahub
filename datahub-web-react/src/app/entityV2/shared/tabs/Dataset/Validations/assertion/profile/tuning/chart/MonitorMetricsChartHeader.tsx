import { Button, Text, Tooltip as TooltipReact, colors } from '@components';
import { DatePicker } from 'antd';
import moment from 'moment';
import { ArrowCounterClockwise } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import {
    ANOMALY_COLOR,
    METRICS_LINE_COLOR,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/constants';

const { RangePicker } = DatePicker;

const DateRangeContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
`;

const StyledRangePicker = styled(RangePicker)`
    .ant-picker-input > input {
        font-size: 12px;
    }

    .ant-picker-separator {
        color: ${colors.gray[400]};
    }

    /* Move calendar icon to the left */
    .ant-picker-suffix {
        order: -1;
        margin-left: 0;
        margin-right: 8px;
    }

    /* Adjust input container to accommodate left-side icon */
    .ant-picker-input {
        display: flex;
        align-items: center;
    }

    border: 1px solid ${colors.gray[100]};
    border-radius: 8px;
    padding: 8px;

    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);

    &:hover {
        border-color: ${colors.primary[400]};
    }

    &.ant-picker-focused {
        border-color: ${colors.primary[500]};
    }
`;

const Legend = styled.div`
    display: flex;
    gap: 16px;
    margin-bottom: 16px;
    font-size: 12px;
`;

const LegendItem = styled(Text)<{ $color: string }>`
    display: flex;
    align-items: center;
    gap: 4px;
    color: ${colors.gray[1700]};

    &::before {
        content: '';
        width: 12px;
        height: 12px;
        border-radius: 100%;
        background-color: ${(props) => props.$color};
        display: block;
    }
`;

const AnomalyLegendItem = styled(Text)`
    display: flex;
    align-items: center;
    gap: 4px;
    color: ${colors.gray[1700]};

    &::before {
        content: '!';
        color: white;
        font-size: 8px;
        font-weight: bold;
        width: 12px;
        height: 12px;
        border-radius: 50%;
        background-color: ${ANOMALY_COLOR};
        display: flex;
        align-items: center;
        justify-content: center;
        text-align: center;
        line-height: 1;
    }
`;

const LegendControlsContainer = styled.div`
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 8px;
`;

const ResetZoomButton = styled(Button)`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const ZoomInstructionText = styled(Text)`
    font-size: 11px;
    color: ${colors.gray[1700]};
`;

type Props = {
    hasAnomalies: boolean;
    futurePredictionsCount: number;
    resetRange?: () => void;
    onRangeChange?: (range: { start: number; end: number }) => void;
    onResetZoom: () => void;
    dateRange: [moment.Moment | null, moment.Moment | null];
    onDateRangeChange: (dates: [moment.Moment | null, moment.Moment | null] | null) => void;
};

export const MonitorMetricsChartHeader: React.FC<Props> = ({
    hasAnomalies,
    futurePredictionsCount,
    resetRange,
    onRangeChange,
    onResetZoom,
    dateRange,
    onDateRangeChange,
}) => {
    return (
        <>
            <DateRangeContainer>
                <StyledRangePicker
                    value={dateRange}
                    onChange={onDateRangeChange}
                    format="MMM DD, YYYY"
                    allowClear={false}
                    placeholder={['Start Date', 'End Date']}
                    size="small"
                />
            </DateRangeContainer>
            <Legend>
                <TooltipReact title="Training metrics that will be used to train the model.">
                    <LegendItem $color={METRICS_LINE_COLOR}>Training Metrics</LegendItem>
                </TooltipReact>
                {hasAnomalies && (
                    <TooltipReact title="Data points flagged as anomalies will be ignored when training the model. To add or remove anomaly flags, go to the main assertion results timeline and change the anomaly state.">
                        <AnomalyLegendItem>Marked as Anomaly</AnomalyLegendItem>
                    </TooltipReact>
                )}
                {futurePredictionsCount > 0 && (
                    <TooltipReact title="Predicted future metric ranges based on the trained model.">
                        <LegendItem $color={colors.primary[200]}>Future Predictions</LegendItem>
                    </TooltipReact>
                )}
                <LegendControlsContainer>
                    {resetRange && (
                        <ResetZoomButton onClick={onResetZoom} variant="secondary">
                            <ArrowCounterClockwise size={12} />
                            Reset Zoom
                        </ResetZoomButton>
                    )}
                    {onRangeChange && !resetRange && <ZoomInstructionText>Drag to zoom in</ZoomInstructionText>}
                </LegendControlsContainer>
            </Legend>
        </>
    );
};
