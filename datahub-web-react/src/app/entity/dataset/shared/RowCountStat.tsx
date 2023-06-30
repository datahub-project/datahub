import React from 'react';
import { TableOutlined } from '@ant-design/icons';
import { formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';
import { countFormatter, needsFormatting } from '../../../../utils/formatter';
import ExpandingStat from './ExpandingStat';
import StatText from './StatText';

type Props = {
    color: string;
    disabled: boolean;
    rowCount?: number | null;
    columnCount?: number | null;
};

const RowCountStat = ({ color, disabled, rowCount, columnCount }: Props) => {
    if (!rowCount) return null;

    return (
        <ExpandingStat
            disabled={disabled || !needsFormatting(rowCount)}
            render={(isExpanded) => (
                <StatText color={color}>
                    <TableOutlined style={{ marginRight: 8, color }} />
                    <b>
                        {isExpanded ? formatNumberWithoutAbbreviation(rowCount) : countFormatter(rowCount as number)}
                    </b>{' '}
                    rows
                    {!!columnCount && (
                        <>
                            ,{' '}
                            <b>
                                {isExpanded
                                    ? formatNumberWithoutAbbreviation(columnCount)
                                    : countFormatter(columnCount)}
                            </b>{' '}
                            columns
                        </>
                    )}
                </StatText>
            )}
        />
    );
};

export default RowCountStat;
