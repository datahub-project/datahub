import { colors } from '@src/alchemy-components/theme';
import React, { forwardRef, useEffect, useRef } from 'react';
import styled from 'styled-components';
import { GLYPH_DROP_SHADOW_FILTER } from './constants';
import { GlyphProps } from './types';

export const ChartWrapper = styled.div`
    width: 100%;
    height: 100%;
    position: relative;
    cursor: pointer;
`;

export const Glyph = ({ x, y }: GlyphProps): React.ReactElement => {
    return (
        <g>
            <circle cx={x} cy={y} r="8" fill={colors.white} filter={GLYPH_DROP_SHADOW_FILTER} />
            <circle cx={x} cy={y} r="6" fill={colors.violet[500]} />
        </g>
    );
};

export const GlyphWithRef = forwardRef<SVGGElement, GlyphProps>((props, ref): React.ReactElement => {
    return (
        <g ref={ref}>
            <Glyph {...props} />
        </g>
    );
});

export const TooltipGlyph = (props: GlyphProps): React.ReactElement => {
    const ref = useRef<SVGGElement>(null);

    // FYI: Change size of parent SVG to prevent showing window's horizontal scrolling
    // There are no any another ways to do it without fixing the library
    useEffect(() => {
        if (ref.current) {
            const parent = ref.current.closest('svg');

            if (parent) {
                parent.setAttribute('width', '1');
                parent.setAttribute('height', '1');
            }
        }
    }, [ref]);

    return <GlyphWithRef {...props} ref={ref} />;
};
