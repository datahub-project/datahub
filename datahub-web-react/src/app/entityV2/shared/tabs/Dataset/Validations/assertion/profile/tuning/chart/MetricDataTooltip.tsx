import { Text, colors } from '@components';
import { Divider } from 'antd';
import { Warning } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import { ANOMALY_COLOR } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/constants';
import {
    MetricDataPoint,
    formatMetricNumber,
    getExclusionWindowForTimestamp,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/utils';

import { AssertionExclusionWindow } from '@types';

const TooltipContainer = styled.div`
    padding: 0;
`;

const AnomalyText = styled(Text)`
    color: ${ANOMALY_COLOR};
`;

const ExclusionWindowText = styled(Text)`
    color: ${colors.gray[600]};
    margin-top: 4px;
`;

type Props = {
    datum: MetricDataPoint;
    exclusionWindows: AssertionExclusionWindow[];
};

export const MetricDataTooltip: React.FC<Props> = ({ datum, exclusionWindows }) => {
    const timestamp = new Date(datum.x).getTime();
    const exclusionWindow = getExclusionWindowForTimestamp(timestamp, exclusionWindows);

    return (
        <TooltipContainer>
            {/* Display the metric value with proper formatting */}
            <Text color="gray" colorLevel={600} size="sm">
                <strong>Value:</strong> {formatMetricNumber(datum.y)}
            </Text>

            {/* Display the timestamp in a human-readable format */}
            <Text color="gray" colorLevel={600} size="sm">
                <strong>Time:</strong> {new Date(datum.x).toLocaleString()}
            </Text>

            {/* Show exclusion window information if the data point falls within one */}
            {exclusionWindow && [
                <Divider key="exclusion-divider" style={{ margin: '8px 0' }} />,
                <ExclusionWindowText key="exclusion-name" color="gray" colorLevel={600} size="sm">
                    <strong>Exclusion window:</strong> &quot;{exclusionWindow.displayName || 'Unnamed exclusion window'}
                    &quot;
                </ExclusionWindowText>,
                <Text key="exclusion-explanation" color="gray" colorLevel={1700} size="sm">
                    This metric will not be used in training.
                </Text>,
            ]}

            {/* Show anomaly warning if data point is anomalous and not in an exclusion window */}
            {datum.hasAnomaly &&
                !exclusionWindow && [
                    <Divider key="anomaly-divider" style={{ margin: '8px 0' }} />,
                    <AnomalyText key="anomaly-warning" size="sm">
                        <strong>
                            {/* Warning icon positioned inline with text */}
                            <Warning
                                size={14}
                                weight="fill"
                                color={colors.yellow[500]}
                                style={{ position: 'relative', top: 2 }}
                            />{' '}
                            Anomaly
                        </strong>
                        <br />
                        {/* Explain impact of anomalous data on training */}
                        This metric will not be used in training.
                        <br />
                        {/* Guide user on how to manage anomalies */}
                        Change this in the main assertion results timeline.
                    </AnomalyText>,
                ]}
        </TooltipContainer>
    );
};
