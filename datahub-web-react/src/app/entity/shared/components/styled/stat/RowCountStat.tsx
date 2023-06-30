import React from 'react';
import { TableOutlined } from '@ant-design/icons';
import StatText from './StatText';
import HorizontalExpander from '../../../../../shared/HorizontalExpander';
import { countFormatter, needsFormatting } from '../../../../../../utils/formatter';
import { formatNumberWithoutAbbreviation } from '../../../../../shared/formatNumber';

type Props = {
    color: string;
    disabled: boolean;
    rowCount: number;
    columnCount?: number | null;
};

const RowCountStat = ({ color, disabled, rowCount, columnCount }: Props) => {
    return (
        <HorizontalExpander
            disabled={disabled || !needsFormatting(rowCount)}
            render={(isExpanded) => (
                <StatText color={color}>
                    <TableOutlined style={{ marginRight: 8, color }} />
                    <b>{isExpanded ? formatNumberWithoutAbbreviation(rowCount) : countFormatter(rowCount)}</b> rows
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
