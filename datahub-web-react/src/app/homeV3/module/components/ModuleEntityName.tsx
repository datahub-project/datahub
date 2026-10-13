import React from 'react';
import styled from 'styled-components';

import DisplayName from '@app/searchV2/autoCompleteV2/components/DisplayName';
import { VARIANT_STYLES } from '@app/searchV2/autoCompleteV2/constants';

const DEFAULT_NAME_STYLES = VARIANT_STYLES.get('default');

const Wrapper = styled.div`
    white-space: nowrap;
    max-width: 100%;
    overflow: hidden;
    color: ${(props) => props.theme.colors.text};
    line-height: 20px;

    & span,
    & div {
        line-height: inherit;
    }
`;

type Props = {
    displayName: string;
    className?: string;
    showNameTooltipIfTruncated?: boolean;
};

export default function ModuleEntityName({ displayName, className, showNameTooltipIfTruncated }: Props) {
    return (
        <Wrapper className={className}>
            <DisplayName
                displayName={displayName}
                weight={DEFAULT_NAME_STYLES?.nameWeight}
                fontSize={DEFAULT_NAME_STYLES?.nameFontSize}
                showNameTooltipIfTruncated={showNameTooltipIfTruncated}
            />
        </Wrapper>
    );
}
