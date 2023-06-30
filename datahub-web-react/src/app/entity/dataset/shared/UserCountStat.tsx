import React from 'react';
import { TeamOutlined } from '@ant-design/icons';
import { formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';
import { countFormatter, needsFormatting } from '../../../../utils/formatter';
import ExpandingStat from './ExpandingStat';
import StatText from './StatText';

type Props = {
    color: string;
    disabled: boolean;
    userCount: number;
};

const UserCountStat = ({ color, disabled, userCount }: Props) => {
    return (
        <ExpandingStat
            disabled={disabled || !needsFormatting(userCount)}
            render={(isExpanded) => (
                <StatText color={color}>
                    <TeamOutlined style={{ marginRight: 8, color }} />
                    <b>{isExpanded ? formatNumberWithoutAbbreviation(userCount) : countFormatter(userCount)}</b> unique
                    users
                </StatText>
            )}
        />
    );
};

export default UserCountStat;
