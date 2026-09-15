import { Icon, spacing } from '@components';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
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
            <Icon icon={Check} size="xl" />
        </Wrapper>
    );
}
