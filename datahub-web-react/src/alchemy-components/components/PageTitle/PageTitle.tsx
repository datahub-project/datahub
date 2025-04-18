import React from 'react';

import { Container, SubTitle, Title } from '@components/components/PageTitle/components';
import { PageTitleProps } from '@components/components/PageTitle/types';
import { Pill } from '@components/components/Pills';

export const PageTitle = ({ title, subTitle, pillLabel, variant = 'pageHeader' }: PageTitleProps) => {
    return (
        <Container>
            <Title data-testid="page-title" variant={variant}>
                {title}
                {pillLabel ? <Pill label={pillLabel} size="sm" clickable={false} /> : null}
            </Title>

            {subTitle ? <SubTitle variant={variant}>{subTitle}</SubTitle> : null}
        </Container>
    );
};
