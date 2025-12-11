/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
