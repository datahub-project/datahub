import React from 'react';
import { HddOutlined } from '@ant-design/icons';
import { formatBytes, formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';
import ExpandingStat from './ExpandingStat';
import { needsFormatting } from '../../../../utils/formatter';
import StatText from './StatText';

type Props = {
    color: string;
    disabled: boolean;
    sizeInBytes: number;
};

export const ByteCountStat = ({ color, disabled, sizeInBytes }: Props) => {
    const formattedBytes = formatBytes(sizeInBytes);

    return (
        <ExpandingStat
            disabled={disabled || !needsFormatting(sizeInBytes)}
            render={(isExpanded) => (
                <StatText color={color}>
                    <HddOutlined style={{ marginRight: 8, color }} />
                    {isExpanded ? (
                        <>
                            <b>{formatNumberWithoutAbbreviation(sizeInBytes)}</b> Bytes
                        </>
                    ) : (
                        <>
                            <b>{formattedBytes.number}</b> {formattedBytes.unit}
                        </>
                    )}
                </StatText>
            )}
        />
    );
};

export default ByteCountStat;
