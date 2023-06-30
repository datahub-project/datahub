import { Popover, Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ClockCircleOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import { toLocalDateTimeString, toRelativeTimeString } from '../../../../../shared/time/timeUtils';
import StatText from './StatText';

type Props = {
    color: string;
    entityLabel: 'chart' | 'dashboard';
    lastUpdatedMs: number;
    createdMs?: number | null;
};

const HelpIcon = styled(QuestionCircleOutlined)<{ color: string }>`
    color: ${(props) => props.color};
    padding-left: 4px;
`;

const LastUpdatedStat = ({ color, entityLabel, lastUpdatedMs, createdMs }: Props) => {
    return (
        <Popover
            mouseEnterDelay={0.5}
            content={
                <>
                    {createdMs && <div>Created on {toLocalDateTimeString(createdMs)}.</div>}
                    <div>
                        Changed on {toLocalDateTimeString(lastUpdatedMs)}.{' '}
                        <Tooltip title={`The time at which the ${entityLabel} was last changed in the source platform`}>
                            <HelpIcon color={color} />
                        </Tooltip>
                    </div>
                </>
            }
        >
            <StatText color={color}>
                <ClockCircleOutlined style={{ marginRight: 8, color }} />
                Changed {toRelativeTimeString(lastUpdatedMs)}
            </StatText>
        </Popover>
    );
};

export default LastUpdatedStat;
