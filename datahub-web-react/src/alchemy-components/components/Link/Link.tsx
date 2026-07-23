import React from 'react';

import { StyledLink } from '@components/components/Link/components';
import { LinkProps, LinkPropsDefaults } from '@components/components/Link/types';

export const linkDefaults: LinkPropsDefaults = {
    color: 'primary',
    colorLevel: 500,
    target: '_blank',
    rel: 'noopener noreferrer',
};

export const Link = ({
    color = linkDefaults.color,
    colorLevel = linkDefaults.colorLevel,
    target = linkDefaults.target,
    rel = linkDefaults.rel,
    children,
    ...props
}: LinkProps) => {
    return (
        <StyledLink color={color} colorLevel={colorLevel} target={target} rel={rel} {...props}>
            {children}
        </StyledLink>
    );
};
