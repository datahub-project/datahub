import React from 'react';
import styled from 'styled-components';

import { Text } from '@src/alchemy-components';

const HeaderWrapper = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
`;

interface Props {
    text: string;
}

export default function SectionHeader({ text }: Props) {
    return (
        <HeaderWrapper>
            <Text weight="semiBold">{text}</Text>
        </HeaderWrapper>
    );
}
