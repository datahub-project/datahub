import React from 'react';

import { StyledDivider } from '@components/components/Divider/components';
import { DividerProps } from '@components/components/Divider/types';

export const dividerDefault: DividerProps = {
    type: 'horizontal',
    dashed: false,
};

export function Divider({ type = dividerDefault.type, dashed = dividerDefault.dashed, ...props }: DividerProps) {
    const dividerType = type || 'horizontal';

    return (
        <StyledDivider
            role="separator"
            aria-orientation={dividerType}
            $type={dividerType}
            $dashed={!!dashed}
            {...props}
        />
    );
}
