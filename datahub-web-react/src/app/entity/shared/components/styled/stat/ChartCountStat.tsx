import React from 'react';
import StatText from './StatText';
import HorizontalExpander from '../../../../../shared/HorizontalExpander';
import { countFormatter, needsFormatting } from '../../../../../../utils/formatter';
import { formatNumberWithoutAbbreviation } from '../../../../../shared/formatNumber';

type Props = {
    color: string;
    chartCount: number;
};

const ChartCountStat = ({ color, chartCount }: Props) => {
    return (
        <HorizontalExpander
            disabled={!needsFormatting(chartCount)}
            render={(isExpanded) => (
                <StatText color={color}>
                    <b>{isExpanded ? formatNumberWithoutAbbreviation(chartCount) : countFormatter(chartCount)}</b>{' '}
                    charts
                </StatText>
            )}
        />
    );
};

export default ChartCountStat;
