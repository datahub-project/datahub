import React from 'react';
import { EyeOutlined } from '@ant-design/icons';
import StatText from './StatText';
import HorizontalExpander from '../../../../../shared/HorizontalExpander';
import { countFormatter, needsFormatting } from '../../../../../../utils/formatter';
import { formatNumberWithoutAbbreviation } from '../../../../../shared/formatNumber';

type Props = {
    color: string;
    viewCount: number;
};

const ViewCountStat = ({ color, viewCount }: Props) => {
    return (
        <HorizontalExpander
            disabled={!needsFormatting(viewCount)}
            render={(isExpanded) => (
                <StatText color={color}>
                    <EyeOutlined style={{ marginRight: 8, color }} />
                    <b>{isExpanded ? formatNumberWithoutAbbreviation(viewCount) : countFormatter(viewCount)}</b> views
                </StatText>
            )}
        />
    );
};

export default ViewCountStat;
