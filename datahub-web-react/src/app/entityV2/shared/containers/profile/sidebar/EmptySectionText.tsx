import React from 'react';
import styled from 'styled-components';

const EmptyContentMessage = styled.span`
    font-size: 12px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textTertiary};
`;

type Props = {
    message: string;
};

const EmptySectionText = ({ message }: Props) => {
    return <EmptyContentMessage>{message}.</EmptyContentMessage>;
};

export default EmptySectionText;
