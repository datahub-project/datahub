import { colors } from '@src/alchemy-components/theme';
import React, { useEffect, useRef } from 'react';
import styled from 'styled-components';
import { GlyphProps } from '@visx/xychart';
import { GLYPH_DROP_SHADOW_FILTER } from './constants';
import { GlyphPropsWithRef } from './types';

export const ChartWrapper = styled.div`
    width: 100%;
    height: 100%;
    position: relative;
    cursor: pointer;
`;

export const Glyph = <T extends object>({ x, y }: GlyphPropsWithRef<T>): React.ReactElement => {
    return (
        <g>
            <circle cx={x} cy={y} r="8" fill={colors.white} filter={GLYPH_DROP_SHADOW_FILTER} />
            <circle cx={x} cy={y} r="6" fill={colors.violet[500]} />
        </g>
    );
};

export const TooltipGlyph = <T extends object>(props: GlyphProps<T>): React.ReactElement => {
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

    return <Glyph {...props} ref={ref} />;
};
