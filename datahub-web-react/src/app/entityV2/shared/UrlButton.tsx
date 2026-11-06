import { ArrowRight } from '@phosphor-icons/react/dist/csr/ArrowRight';
import React, { ReactNode } from 'react';
import styled from 'styled-components/macro';

const UrlButtonContainer = styled.span`
    font-size: 12px;
`;

// Plain anchor: alchemy Button renders a <button> and has no href support.
const StyledLink = styled.a`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 4px 12px 0 12px;
    font-size: 14px;
    color: ${(props) => props.theme.colors.textBrand};

    > :hover {
        text-decoration: underline;
    }
`;

interface Props {
    href: string;
    children: ReactNode;
    onClick?: () => void;
}

const NOOP = () => {};

export default function UrlButton({ href, children, onClick = NOOP }: Props) {
    return (
        <UrlButtonContainer>
            <StyledLink href={href} target="_blank" rel="noreferrer noopener" onClick={onClick}>
                {children} <ArrowRight style={{ fontSize: 12 }} />
            </StyledLink>
        </UrlButtonContainer>
    );
}
