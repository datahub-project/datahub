import React from 'react';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { OverflowText } from '@components/components/OverflowText/OverflowText';
import { Container, SubTitle, Title } from '@components/components/PageTitle/components';
import { PageTitleProps } from '@components/components/PageTitle/types';
import { Pill } from '@components/components/Pills';
import { Tooltip } from '@components/components/Tooltip';

const Wrapper = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
`;

export const PageTitle = ({
    title,
    subTitle,
    pillLabel,
    variant = 'pageHeader',
    actionButton,
    titlePill,
}: PageTitleProps) => {
    return (
        <Wrapper style={actionButton ? { width: '100%' } : {}}>
            <Container>
                <Title data-testid="page-title" variant={variant}>
                    <OverflowText text={title} />
                    {pillLabel ? <Pill label={pillLabel} size="sm" clickable={false} /> : null}
                    {titlePill}
                </Title>

                {subTitle ? <SubTitle variant={variant}>{subTitle}</SubTitle> : null}
            </Container>
            {actionButton ? <ActionButton {...actionButton} /> : null}
        </Wrapper>
    );
};

function ActionButton({ label, tooltip, ...buttonProps }: NonNullable<PageTitleProps['actionButton']>) {
    const button = (
        <Button size="md" {...buttonProps}>
            {label}
        </Button>
    );

    if (!tooltip) {
        return button;
    }

    return (
        <Tooltip title={tooltip} showArrow={false}>
            <span>{button}</span>
        </Tooltip>
    );
}
