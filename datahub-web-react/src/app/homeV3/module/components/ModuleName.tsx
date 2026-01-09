import { colors } from '@components';
import React from 'react';

import { NameContainer } from '@app/homeV3/styledComponents';

interface Props {
    text?: string;
}

export default function ModuleName({ text }: Props) {
    return (
        <NameContainer
            ellipsis={{
                tooltip: {
                    color: 'white',
                    overlayInnerStyle: { color: colors.gray[1700] },
                    showArrow: false,
                },
            }}
        >
            {text}
        </NameContainer>
    );
}
