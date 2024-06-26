/* 
 Docs Only Component that helps to display a list of components in a grid layout.
*/

import React from 'react';

const styles = {
	display: 'flex',
	alignItems: 'center',
	justifyContent: 'center',
	gap: '8px',
}

export const GridList = ({ children }) => {
	return (
		<div style={styles}>
			{children}
		</div>
	);
}