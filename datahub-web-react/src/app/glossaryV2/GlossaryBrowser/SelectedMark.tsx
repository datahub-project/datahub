import { Icon, spacing } from '@components';
import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    padding-left: ${spacing.sm};
    padding-right: ${spacing.sm};
    color: ${(props) => props.theme.colors.iconSelected};
`;

interface Props {
    className?: string;
}

export function SelectedMark({ className }: Props) {
    return (
        <Wrapper className={className}>
            <Icon source="phosphor" icon="Check" size="xl" />
        </Wrapper>
    );
}
