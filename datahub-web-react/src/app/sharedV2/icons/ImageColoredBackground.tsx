/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import ColorThief from 'colorthief';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

const Wrapper = styled.div<{ background?: string }>`
    align-items: center;
    background-color: ${({ background }) => background || 'transparent'};
    display: flex;
    justify-content: center;
`;

interface Props {
    children?: JSX.Element;
    className?: string;
}

export default function ImageColoredBackground({ children, className }: Props): JSX.Element {
    const [background, setBackground] = React.useState<string | undefined>(undefined);

    const ref = useCallback(
        (node: HTMLDivElement) => {
            if (node === null) {
                return;
            }
            const image = node.getElementsByTagName('img')[0];
            if (image) {
                const colorThief = new ColorThief();
                try {
                    const [r, g, b] = colorThief.getColor(image, 25);
                    setBackground(`rgb(${r}, ${g}, ${b}, .1)`);
                } catch (_e) {
                    image.onload = () => {
                        if (background !== undefined) {
                            return;
                        }
                        const [r, g, b] = colorThief.getColor(image, 25);
                        setBackground(`rgb(${r}, ${g}, ${b}, .1)`);
                        image.crossOrigin = 'anonymous';
                    };
                    image.onerror = () => {
                        image.removeAttribute('crossOrigin');
                        setBackground(REDESIGN_COLORS.BACKGROUND_GREY);
                    };
                }
            }
            const svg = node.getElementsByTagName('svg')[0];
            if (svg) {
                const color = window.getComputedStyle(svg).getPropertyValue('color');
                const [, r, g, b] = color.match(/^rgb\((\d{1,3}),\s*(\d{1,3}),\s*(\d{1,3})\)$/) || [0, 0, 0];
                setBackground(`rgb(${r}, ${g}, ${b}, .1)`);
            }
        },
        [background],
    );

    return (
        <Wrapper ref={ref} background={background} className={className}>
            {children}
        </Wrapper>
    );
}
