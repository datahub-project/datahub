import React from 'react';
import { useTheme } from 'styled-components';

import { NameContainer } from '@app/homeV3/styledComponents';

interface Props {
    text?: string;
}

export default function ModuleName({ text }: Props) {
    const theme = useTheme();
    return (
        <NameContainer
            ellipsis={{
                tooltip: {
                    overlayInnerStyle: { color: theme.colors.textSecondary },
                    showArrow: false,
                },
            }}
        >
            {text}
        </NameContainer>
    );
}
