import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

interface Props {
    name: string;
    description?: string;
}

export function SectionName({ name, description }: Props) {
    return (
        <Wrapper>
            <Text weight="semiBold" size="lg">
                {name}
            </Text>
            {description && <Text color="gray">{description}</Text>}
        </Wrapper>
    );
}
