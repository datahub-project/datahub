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
