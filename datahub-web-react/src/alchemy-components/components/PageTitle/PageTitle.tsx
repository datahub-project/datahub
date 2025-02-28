import React from 'react';
import { PageTitleProps } from './types';
import { Container, SubTitle, Title } from './components';
import { Pill } from '../Pills';

export const PageTitle = ({ title, subTitle, pillLabel, variant = 'pageHeader' }: PageTitleProps) => {
    return (
        <Container>
            <Title variant={variant}>
                {title}
                {pillLabel ? <Pill label={pillLabel} size="sm" clickable={false} /> : null}
            </Title>

            {subTitle ? <SubTitle variant={variant}>{subTitle}</SubTitle> : null}
        </Container>
    );
};
