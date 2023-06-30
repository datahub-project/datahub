import React from 'react';
import { ConsoleSqlOutlined } from '@ant-design/icons';
import StatText from './StatText';
import HorizontalExpander from '../../../../../shared/HorizontalExpander';
import { countFormatter, needsFormatting } from '../../../../../../utils/formatter';
import { formatNumberWithoutAbbreviation } from '../../../../../shared/formatNumber';

type Props = {
    color: string;
    disabled: boolean;
    totalSqlQueries?: number | null;
    queryCountLast30Days?: number | null;
};

const QueryCountStat = ({ color, disabled, totalSqlQueries, queryCountLast30Days }: Props) => {
    const queryCount = queryCountLast30Days || totalSqlQueries;

    if (!queryCount) return null;

    return (
        <HorizontalExpander
            disabled={disabled || !needsFormatting(queryCount)}
            render={(isExpanded) => (
                <StatText color={color}>
                    <ConsoleSqlOutlined style={{ marginRight: 8, color }} />
                    <b>{isExpanded ? formatNumberWithoutAbbreviation(queryCount) : countFormatter(queryCount)}</b>{' '}
                    {queryCountLast30Days ? <>queries last month</> : <>monthly queries</>}
                </StatText>
            )}
        />
    );
};

export default QueryCountStat;
