import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    color: ${colors.red[500]};
    margin-top: 5px;
`;

interface Props {
    errors: React.ReactNode[];
}

export function ErrorWrapper({ errors }: Props) {
    return <Wrapper>{errors}</Wrapper>;
}
