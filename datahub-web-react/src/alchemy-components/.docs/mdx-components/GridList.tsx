/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
/* 
 Docs Only Component that helps to display a list of components in a grid layout.
*/
import React, { ReactNode } from 'react';

const styles = {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    gap: '8px',
};

interface Props {
    isVertical?: boolean;
    width?: number | string;
    children: ReactNode;
}

export const GridList = ({ isVertical = false, width = '100%', children }: Props) => {
    return (
        <div
            style={{
                ...styles,
                width,
                flexDirection: isVertical ? 'column' : 'row',
            }}
        >
            {children}
        </div>
    );
};
