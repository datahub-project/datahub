import React from 'react';
import { useTheme } from 'styled-components';

import { NameContainer } from '@app/homeV3/styledComponents';

interface Props {
    text?: string;
    dataTestId?: string;
}

export default function ModuleName({ text, dataTestId }: Props) {
    const theme = useTheme();
    return (
        <NameContainer
            ellipsis={{
                tooltip: {
                    overlayInnerStyle: { color: theme.colors.textSecondary },
                    showArrow: false,
                },
            }}
            data-testid={dataTestId}
        >
            {text}
        </NameContainer>
    );
}
