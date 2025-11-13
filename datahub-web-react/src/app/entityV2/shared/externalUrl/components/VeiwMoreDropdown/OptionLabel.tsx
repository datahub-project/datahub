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
