import React from 'react';
import { Link, LinkProps } from 'react-router-dom';
import styled from 'styled-components';

import { useGetModalLinkProps } from '@app/sharedV2/modals/useGetModalLinkProps';

const StyledLink = styled(Link)`
    display: block;
    min-width: 0;
    color: ${(props) => props.theme.colors.text};
    text-decoration: none;

    &:hover,
    &:focus,
    &:active,
    &:visited {
        color: ${(props) => props.theme.colors.text};
        text-decoration: none;
        cursor: pointer;
    }
`;

export default function ModuleEntityLink({ children, ...linkProps }: LinkProps) {
    const modalLinkProps = useGetModalLinkProps();
    return (
        <StyledLink {...linkProps} {...modalLinkProps}>
            {children}
        </StyledLink>
    );
}
