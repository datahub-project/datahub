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
    // eslint-disable-next-line i18next/no-literal-string -- trailing punctuation appended uniformly to all empty-state messages
    return <EmptyContentMessage>{message}.</EmptyContentMessage>;
};

export default EmptySectionText;
