/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { Popover, zIndices } from '@src/alchemy-components';
import useMeasureIfTrancated from '@src/app/shared/useMeasureIfTruncated';

const LabelWrapper = styled.div`
    max-width: 300px;
    text-wrap: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const PopoverWrapper = styled.div`
    max-width: 300px;
    overflow-wrap: break-word;
`;

interface Props {
    text: string;
    dataTestId?: string;
}

export default function OptionLabel({ text, dataTestId }: Props) {
    const { measuredRef, isHorizontallyTruncated } = useMeasureIfTrancated();

    return (
        <Popover
            zIndex={zIndices.popover}
            content={isHorizontallyTruncated ? <PopoverWrapper>{text}</PopoverWrapper> : undefined}
        >
            <LabelWrapper ref={measuredRef} data-testid={dataTestId}>
                {text}
            </LabelWrapper>
        </Popover>
    );
}
