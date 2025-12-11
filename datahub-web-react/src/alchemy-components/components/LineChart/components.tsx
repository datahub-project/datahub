/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { forwardRef, useEffect, useRef } from 'react';
import styled from 'styled-components';

import { GLYPH_DROP_SHADOW_FILTER } from '@components/components/LineChart/constants';
import { GlyphProps } from '@components/components/LineChart/types';

import { colors } from '@src/alchemy-components/theme';

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
