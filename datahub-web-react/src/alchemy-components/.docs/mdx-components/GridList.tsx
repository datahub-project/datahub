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
    children: ReactNode;
}

export const GridList = ({ children }: Props) => {
    return <div style={styles}>{children}</div>;
};
