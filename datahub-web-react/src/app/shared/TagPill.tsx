import React from 'react';

import { StyledTag } from '../entity/shared/components/styled/StyledTag';

type Props = {
    suggestion: string;
    colorHash;
    color;
};

export default function TagPill({ suggestion, colorHash, color }: Props) {
    return (
        <StyledTag
            $colorHash={colorHash}
            $color={color}
            closable={false}
            style={{
                border: 'none',
                marginLeft: '-2px',
                fontSize: '10px',
                lineHeight: '20px',
                whiteSpace: 'nowrap',
                marginRight: '-10px',
                opacity: 1,
                color: '#434343',
            }}
        >
            {suggestion}
        </StyledTag>
    );
}
