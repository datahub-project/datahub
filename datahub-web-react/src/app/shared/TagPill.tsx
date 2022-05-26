import React from 'react';

import { StyledTag } from '../entity/shared/components/styled/StyledTag';

type Props = {
    name: string;
    colorHash;
    color;
    style;
};

export default function TagPill({ name, colorHash, color, style }: Props) {
    return (
        <StyledTag $colorHash={colorHash} $color={color} closable={false} style={style}>
            {name}
        </StyledTag>
    );
}
