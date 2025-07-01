import React from 'react';
import styled from 'styled-components';

import { OverflowText } from '@components/components/OverflowText/OverflowText';
import { Container, SubTitle, Title } from '@components/components/PageTitle/components';
import { PageTitleProps } from '@components/components/PageTitle/types';
import { Pill } from '@components/components/Pills';

const StyledSubTitle = styled(SubTitle)`
    margin-top: 4px;
`;

export const PageTitle = ({ title, subTitle, pillLabel, variant = 'pageHeader' }: PageTitleProps) => {
    return (
        <Container>
            <Title data-testid="page-title" variant={variant}>
                <OverflowText text={title} />
                {pillLabel ? <Pill label={pillLabel} size="sm" clickable={false} /> : null}
            </Title>

            {subTitle ? <StyledSubTitle variant={variant}>{subTitle}</StyledSubTitle> : null}
        </Container>
    );
};
