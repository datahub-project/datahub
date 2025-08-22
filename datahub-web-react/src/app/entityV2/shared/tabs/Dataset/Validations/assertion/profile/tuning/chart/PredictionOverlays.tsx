import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { AssertionPrediction } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/useInferenceRegenerationPoller';
import { MARGIN } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/constants';
import {
    calculatePredictionOverlayPosition,
    calculatePredictionOverlayYPosition,
    calculateTooltipPosition,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/utils';

const PredictionOverlay = styled.div<{
    leftPercent: number;
    widthPercent: number;
    topPercent: number;
    heightPercent: number;
    width: number;
    height: number;
    isHovered: boolean;
}>`
    position: absolute;
    left: ${(props) => MARGIN.left + (props.leftPercent / 100) * (props.width - MARGIN.left - MARGIN.right)}px;
    top: ${(props) => MARGIN.top + (props.topPercent / 100) * (props.height - MARGIN.top - MARGIN.bottom)}px;
    width: ${(props) => (props.widthPercent / 100) * (props.width - MARGIN.left - MARGIN.right)}px;
    height: ${(props) => (props.heightPercent / 100) * (props.height - MARGIN.top - MARGIN.bottom)}px;
    background-color: ${colors.primary[200]};
    opacity: ${(props) => (props.isHovered ? 0.5 : 0.3)};
    border-top: 1px dashed ${colors.primary[500]};
    border-bottom: 1px dashed ${colors.primary[500]};
    z-index: 1;
    cursor: pointer;
    transition: opacity 0.2s ease;
`;

type Props = {
    futurePredictions: AssertionPrediction[];
    xAxisDomain: [Date, Date];
    yAxisDomain: [number, number];
    width: number;
    height: number;
    hoveredPrediction: {
        prediction: AssertionPrediction;
        mousePosition: { x: number; y: number };
    } | null;
    onPredictionHover: (prediction: AssertionPrediction, mousePosition: { x: number; y: number }) => void;
    onPredictionLeave: () => void;
};

export const PredictionOverlays: React.FC<Props> = ({
    futurePredictions,
    xAxisDomain,
    yAxisDomain,
    width,
    height,
    hoveredPrediction,
    onPredictionHover,
    onPredictionLeave,
}) => {
    if (futurePredictions.length === 0) {
        return null;
    }

    return (
        <>
            {futurePredictions.map((prediction) => {
                if (!prediction.timeWindow?.startTimeMillis || !prediction.timeWindow?.endTimeMillis) {
                    return null;
                }

                // Calculate the position and width for the prediction overlay
                const {
                    leftPercent,
                    widthPercent,
                    isVisible: isVisibleX,
                } = calculatePredictionOverlayPosition(
                    prediction.timeWindow.startTimeMillis,
                    prediction.timeWindow.endTimeMillis,
                    xAxisDomain,
                );

                // Calculate the Y position and height based on prediction bounds
                const {
                    topPercent,
                    heightPercent,
                    isVisible: isVisibleY,
                } = calculatePredictionOverlayYPosition(prediction, yAxisDomain);

                // Only show if the prediction is visible in both X and Y domains
                if (!isVisibleX || !isVisibleY) {
                    return null;
                }

                return (
                    <PredictionOverlay
                        key={`prediction-overlay-${prediction.timeWindow.startTimeMillis}`}
                        leftPercent={leftPercent}
                        widthPercent={widthPercent}
                        topPercent={topPercent}
                        heightPercent={heightPercent}
                        width={width}
                        height={height}
                        isHovered={hoveredPrediction?.prediction === prediction}
                        onMouseEnter={(e) => {
                            const predictionRect = e.currentTarget.getBoundingClientRect();
                            const chartContainer = e.currentTarget.parentElement;

                            if (!chartContainer) return;

                            const chartRect = chartContainer.getBoundingClientRect();
                            const mousePosition = calculateTooltipPosition(predictionRect, chartRect, height, MARGIN);

                            onPredictionHover(prediction, mousePosition);
                        }}
                        onMouseLeave={onPredictionLeave}
                    />
                );
            })}
        </>
    );
};
