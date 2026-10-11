import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    color: ${(props) => props.theme.colors.textError};
    margin-top: 5px;
`;

interface Props {
    errors: React.ReactNode[];
}

export function ErrorWrapper({ errors }: Props) {
    return <Wrapper>{errors}</Wrapper>;
}
