import { Popover } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ClockCircleOutlined } from '@ant-design/icons';
import { toLocalDateTimeString, toRelativeTimeString } from '../../../../../shared/time/timeUtils';
import StatText from './StatText';

type Props = {
    color: string;
    disabled?: boolean;
    lastUpdatedMs: number;
    createdMs?: number | null;
};

const PopoverContent = styled.div`
    max-width: 300px;
`;

const LastUpdatedStat = ({ color, disabled = false, lastUpdatedMs, createdMs }: Props) => {
    return (
        <Popover
            open={disabled ? false : undefined}
            mouseEnterDelay={0.5}
            content={
                <PopoverContent>
                    Changed <strong>{toLocalDateTimeString(lastUpdatedMs)}</strong>
                    {createdMs && (
                        <>
                            <br />
                            Created on <strong>{toLocalDateTimeString(createdMs)}</strong>
                        </>
                    )}
                </PopoverContent>
            }
        >
            <StatText color={color}>
                <ClockCircleOutlined style={{ marginRight: 8, color }} />
                Updated {toRelativeTimeString(lastUpdatedMs)}
            </StatText>
        </Popover>
    );
};

export default LastUpdatedStat;
