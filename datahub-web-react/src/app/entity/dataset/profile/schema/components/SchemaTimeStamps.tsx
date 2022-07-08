import { ClockCircleOutlined } from '@ant-design/icons';
import { Popover, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { toLocalDateTimeString, toRelativeTimeString } from '../../../../../shared/time/timeUtils';
import { ANTD_GRAY } from '../../../../shared/constants';

const CurrentVersionTimestampText = styled(Typography.Text)`
    &&& {
        line-height: 22px;
        margin-top: 10px;
        margin-right: 10px;
        color: ${ANTD_GRAY[7]};
        width: max-content;
    }
`;

const TimeStampWrapper = styled.div`
    margin-bottom: 5px;
`;

const StyledClockIcon = styled(ClockCircleOutlined)`
    margin-right: 5px;
`;

interface Props {
    lastUpdated?: number | null;
    lastObserved?: number | null;
}

function SchemaTimeStamps(props: Props) {
    const { lastUpdated, lastObserved } = props;

    if (!lastUpdated && !lastObserved) return null;

    return (
        <Popover
            content={
                <>
                    {lastObserved && (
                        <TimeStampWrapper>Last observed on {toLocalDateTimeString(lastObserved)}.</TimeStampWrapper>
                    )}
                    {lastUpdated && <div>First reported on {toLocalDateTimeString(lastUpdated)}.</div>}
                </>
            }
        >
            <CurrentVersionTimestampText>
                {lastObserved && (
                    <span>
                        <StyledClockIcon /> Last observed {toRelativeTimeString(lastObserved)}
                    </span>
                )}
                {!lastObserved && lastUpdated && (
                    <span>
                        <StyledClockIcon />
                        Reported {toRelativeTimeString(lastUpdated)}
                    </span>
                )}
            </CurrentVersionTimestampText>
        </Popover>
    );
}

export default SchemaTimeStamps;
