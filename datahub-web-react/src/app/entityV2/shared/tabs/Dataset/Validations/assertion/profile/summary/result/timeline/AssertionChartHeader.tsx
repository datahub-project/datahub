import { Button, Text } from '@components';
import { Typography, message } from 'antd';
import { Sparkle } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import {
    AssertionChartType,
    TimeRange,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/types';
import { getBestChartTypeForAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/utils';
import { getTimeRangeDisplay } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/utils';
import { getIsSmartAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/assertionUtils';

import { Assertion, Maybe, Monitor } from '@types';

const VIZ_CONTAINER_TITLE_HEIGHT = 36;

const VizHeader = styled.div`
    width: 100%;
    height: ${VIZ_CONTAINER_TITLE_HEIGHT}px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    gap: 12px;
    margin-bottom: 20px;
    margin-top: 4px;
`;

const VizHeaderTitle = styled(Typography.Text)`
    color: ${ANTD_GRAY[9]};
    font-size: 16px;
    font-weight: 600;
`;

type Props = {
    title?: string;
    timeRange: TimeRange;
    assertion: Assertion;
    monitor?: Maybe<Monitor>;
    onOpenTunePredictionsModal: () => void;
};

const AssertionChartHeader = ({ title, timeRange, assertion, monitor, onOpenTunePredictionsModal }: Props) => {
    const isSmartAssertion = getIsSmartAssertion(assertion);
    const bestChartType = getBestChartTypeForAssertion(assertion.info);
    return (
        <VizHeader>
            <VizHeaderTitle strong>{title || getTimeRangeDisplay(timeRange)}</VizHeaderTitle>
            {isSmartAssertion && bestChartType === AssertionChartType.ValuesOverTime && (
                <Button
                    variant="secondary"
                    size="sm"
                    onClick={() => {
                        if (!monitor) {
                            message.error('Could not find the monitor for this assertion.');
                        } else {
                            onOpenTunePredictionsModal();
                        }
                    }}
                >
                    <Sparkle weight="fill" size={12} />
                    <Text>Tune Predictions</Text>
                </Button>
            )}
        </VizHeader>
    );
};

export default AssertionChartHeader;
