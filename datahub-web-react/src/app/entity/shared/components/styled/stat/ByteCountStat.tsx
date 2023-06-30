import React from 'react';
import { HddOutlined } from '@ant-design/icons';
import StatText from './StatText';
import { formatBytes, formatNumberWithoutAbbreviation } from '../../../../../shared/formatNumber';
import { needsFormatting } from '../../../../../../utils/formatter';
import HorizontalExpander from '../../../../../shared/HorizontalExpander';

type Props = {
    color: string;
    disabled: boolean;
    sizeInBytes: number;
};

const ByteCountStat = ({ color, disabled, sizeInBytes }: Props) => {
    const formattedBytes = formatBytes(sizeInBytes);

    return (
        <HorizontalExpander
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
