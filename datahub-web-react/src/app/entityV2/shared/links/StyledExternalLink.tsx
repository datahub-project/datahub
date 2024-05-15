import React, { ReactNode } from 'react';
import styled from 'styled-components';

interface Props {
    url: string | null | undefined;
    children: ReactNode;
}

const StyledlLink = styled.a`
    display: flex;
    align-items: center;
    gap: 5px;
    background: #eeecfa;
    padding: 4px 6px;
    border-radius: 4px;
    color: #533fd1;
`;

const StyledExternalLink: React.FC<Props> = ({ url, children }) => (
    <StyledlLink href={url || undefined} target="_blank">
        {children}
    </StyledlLink>
);

export default StyledExternalLink;
