import React from 'react';
import { PageTitleProps } from './types';
import { Container, SubTitle, Title } from './components';
import { Pill } from '../Pills';

export const PageTitle = ({ title, subTitle, pillLabel }: PageTitleProps) => {
    return (
        <Container>
            <Title>
                {title}
                {pillLabel ? <Pill label={pillLabel} size="sm" clickable={false} /> : null}
            </Title>

            {subTitle ? <SubTitle>{subTitle}</SubTitle> : null}
        </Container>
    );
};
