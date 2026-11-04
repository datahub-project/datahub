import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useBuildAssertionDescriptionLabels } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/utils';

import { Assertion } from '@types';

// alchemy Text sets its own font-size/line-height; antd Typography.Text inherited them
const InlineText = styled(Text).attrs({ type: 'span' })`
    color: ${(props) => props.theme.colors.text};
    font-size: inherit;
    line-height: inherit;
    word-break: break-word;
`;

type Props = {
    assertion: Assertion;
    options?: {
        noSecondarySpacing?: boolean;
        showColumnTag?: boolean;
        hideSecondaryLabel?: boolean;
    };
};

// Component useful for rendering descriptions of assertions.
export const AssertionDescription = ({ assertion, options }: Props) => {
    const assertionInfo = assertion.info;
    const { primaryLabel, secondaryLabel } = useBuildAssertionDescriptionLabels(assertionInfo, {
        showColumnTag: options?.showColumnTag,
    });

    return (
        <>
            {primaryLabel}
            <div>
                {!options?.hideSecondaryLabel && (
                    <InlineText style={{ marginLeft: options?.noSecondarySpacing ? 0 : 0 }}>
                        {secondaryLabel}
                    </InlineText>
                )}
            </div>
        </>
    );
};
