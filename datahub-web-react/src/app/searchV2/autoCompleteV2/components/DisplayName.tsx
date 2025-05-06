import { MatchText, Popover, zIndices } from '@components';
import React from 'react';
import styled from 'styled-components';

import { NAME_COLOR, NAME_COLOR_LEVEL } from '@app/searchV2/autoCompleteV2/constants';
import useMeasureIfTrancated from '@app/shared/useMeasureIfTruncated';

const EntityTitleContainer = styled.div`
    text-overflow: ellipsis;
    overflow: hidden;
    max-width: 400px;
`;

const PopoverWrapper = styled.div`
    max-width: 500px;
    overflow-wrap: break-word;
`;

interface Props {
    displayName: string;
    highlight?: string;
}

export default function DisplayName({ displayName, highlight }: Props) {
    const { measuredRef, isHorizontallyTruncated } = useMeasureIfTrancated();

    return (
        <Popover
            zIndex={zIndices.popover}
            content={isHorizontallyTruncated ? <PopoverWrapper>{displayName}</PopoverWrapper> : undefined}
        >
            <EntityTitleContainer ref={measuredRef}>
                <MatchText
                    type="span"
                    color={NAME_COLOR}
                    colorLevel={NAME_COLOR_LEVEL}
                    text={displayName}
                    highlight={highlight ?? ''}
                />
            </EntityTitleContainer>
        </Popover>
    );
}
