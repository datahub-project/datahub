import { Icon, IconNames, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import spacing from '@components/theme/foundations/spacing';

const Wrapper = styled.div`
    display: flex;
    gap: ${spacing.xsm};
    padding: ${spacing.xsm};
    align-items: center;
`;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    text-overflow: ellipsis;
    word-wrap: nowrap;
`;

const IconWrapper = styled.div`
    display: flex;
    flex-shrink: 0;
`;

const SpaceFiller = styled.div`
    flex-grow: 1;
`;

interface Props {
    icon: IconNames;
    title: string;
    description?: string;
    hasChildren?: boolean;
}

export default function MenuItem({ icon, title, description, hasChildren }: Props) {
    return (
        <Wrapper>
            <IconWrapper>
                <Icon icon={icon} source="phosphor" color="gray" size="2xl" />
            </IconWrapper>

            <Container>
                <Text weight="semiBold">{title}</Text>
                {description && (
                    <Text color="gray" colorLevel={1700} size="sm">
                        {description}
                    </Text>
                )}
            </Container>

            <SpaceFiller />

            {hasChildren && <Icon icon="CaretRight" source="phosphor" color="gray" size="lg" />}
        </Wrapper>
    );
}
