import React from 'react';

import DeprecatedIcon from '@images/deprecated-status.svg?react';

// The raw deprecated-status SVG has a 16x16 viewBox with artwork filling 100%, while phosphor
// icons used elsewhere in the menu reserve ~20% padding inside their viewBox. Without scaling,
// this icon appears visibly larger than its peers in action menus.
interface DeprecatedMenuIconProps extends React.SVGProps<SVGSVGElement> {
    style?: React.CSSProperties;
}

export const DeprecatedMenuIcon = ({ style, ...rest }: DeprecatedMenuIconProps) => (
    <DeprecatedIcon {...rest} style={{ ...style, width: '80%', height: '80%' }} />
);
