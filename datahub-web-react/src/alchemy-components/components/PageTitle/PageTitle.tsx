/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { OverflowText } from '@components/components/OverflowText/OverflowText';
import { Container, SubTitle, Title } from '@components/components/PageTitle/components';
import { PageTitleProps } from '@components/components/PageTitle/types';
import { Pill } from '@components/components/Pills';

const Wrapper = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
`;

export const PageTitle = ({ title, subTitle, pillLabel, variant = 'pageHeader', actionButton }: PageTitleProps) => {
    return (
        <Wrapper style={actionButton ? { width: '100%' } : {}}>
            <Container>
                <Title data-testid="page-title" variant={variant}>
                    <OverflowText text={title} />
                    {pillLabel ? <Pill label={pillLabel} size="sm" clickable={false} /> : null}
                </Title>

                {subTitle ? <SubTitle variant={variant}>{subTitle}</SubTitle> : null}
            </Container>
            {actionButton ? (
                <Button onClick={actionButton.onClick} size="md">
                    {actionButton.icon && actionButton.icon}
                    {actionButton.label}
                </Button>
            ) : null}
        </Wrapper>
    );
};
