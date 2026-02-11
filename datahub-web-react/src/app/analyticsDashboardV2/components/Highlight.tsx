import { Card, Tooltip } from '@components';
import React from 'react';

import { formatNumber } from '@app/shared/formatNumber';

import { Highlight as HighlightType } from '@types';

type Props = {
    highlight: HighlightType;
    shortenValue?: boolean;
};

export const Highlight = ({ highlight, shortenValue }: Props) => {
    return (
        <Tooltip title={highlight.body}>
            <div>
                <Card
                    title={(shortenValue && formatNumber(highlight.value)) || highlight.value}
                    subTitle={highlight.title}
                />
            </div>
        </Tooltip>
    );
};
