import React from 'react';
import { TeamOutlined } from '@ant-design/icons';
import StatText from './StatText';
import HorizontalExpander from '../../../../../shared/HorizontalExpander';
import { countFormatter, needsFormatting } from '../../../../../../utils/formatter';
import { formatNumberWithoutAbbreviation } from '../../../../../shared/formatNumber';

type Props = {
    color: string;
    disabled?: boolean;
    userCount: number;
};

const UserCountStat = ({ color, disabled = false, userCount }: Props) => {
    return (
        <HorizontalExpander
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
