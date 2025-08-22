// External dependencies
import { Text, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

// Internal dependencies
import { AssertionPrediction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/useInferenceRegenerationPoller';
import {
    formatMetricNumber,
    formatTooltipDate,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/utils';

/**
 * Main tooltip container positioned absolutely at mouse coordinates
 * Features rounded corners, shadow, and is centered vertically
 */
const StyledPredictionTooltip = styled.div<{ x: number; y: number }>`
    position: absolute;
    left: ${(props) => props.x}px;
    top: ${(props) => props.y}px;
    background: white;
    border: 1px solid ${colors.gray[100]};
    border-radius: 6px;
    padding: 8px 12px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    z-index: 1000;
    pointer-events: none;
    font-size: 12px;
    line-height: 1.4;
    transform: translateY(-50%); // Center the tooltip vertically
`;

/**
 * Header component for the tooltip title
 * Styled with semibold weight and bottom margin
 */
const TooltipHeader = styled(Text)`
    margin-bottom: 4px;
    font-weight: 600;
    color: ${colors.gray[600]};
`;

/**
 * Text component for displaying upper and lower bound values
 * Includes small bottom margin for spacing between bounds
 */
const TooltipBoundText = styled(Text)`
    margin-bottom: 2px;
    color: ${colors.gray[600]};
`;

/**
 * Text component for displaying time window information
 * Smaller font size and prevents text wrapping
 */
const TooltipTimeText = styled(Text)`
    font-size: 11px;
    color: ${colors.gray[600]};
    white-space: nowrap;
`;

/**
 * Props interface for the FuturePredictionTooltip component
 */
type Props = {
    /** Assertion prediction data containing bounds and time window */
    prediction: AssertionPrediction;
    /** Current mouse coordinates for tooltip positioning */
    mousePosition: { x: number; y: number };
};

/**
 * Tooltip component that displays future prediction information for assertion metrics
 * Shows upper/lower bounds and optional time window when hovering over prediction data points
 */
export const FuturePredictionTooltip: React.FC<Props> = ({ prediction, mousePosition }) => {
    return (
        <StyledPredictionTooltip x={mousePosition.x} y={mousePosition.y}>
            {/* Tooltip title */}
            <TooltipHeader color="gray" colorLevel={600} size="sm">
                Future Prediction
            </TooltipHeader>

            {/* Display upper bound value */}
            <TooltipBoundText color="gray" colorLevel={600} size="sm">
                <strong>Upper Bound:</strong> {formatMetricNumber(prediction.upperBound || 0)}
            </TooltipBoundText>

            {/* Display lower bound value */}
            <TooltipBoundText color="gray" colorLevel={600} size="sm">
                <strong>Lower Bound:</strong> {formatMetricNumber(prediction.lowerBound || 0)}
            </TooltipBoundText>

            {/* Conditionally display time window if available */}
            {prediction.timeWindow?.startTimeMillis && (
                <TooltipTimeText color="gray" colorLevel={600} size="sm">
                    {formatTooltipDate(prediction.timeWindow.startTimeMillis)}
                    {/* Show end time if it exists and differs from start time */}
                    {prediction.timeWindow?.endTimeMillis &&
                        prediction.timeWindow.endTimeMillis !== prediction.timeWindow.startTimeMillis && (
                            <> - {formatTooltipDate(prediction.timeWindow.endTimeMillis)}</>
                        )}
                </TooltipTimeText>
            )}
        </StyledPredictionTooltip>
    );
};
