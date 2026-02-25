import React from 'react';

import {
    CardContainer,
    SubTitle,
    SubTitleContainer,
    Title,
    TitleContainer,
} from '@components/components/Card/components';
import { Switch } from '@components/components/Switch';
import { Header } from '@components/components/ToggleCard/components';
import { ToggleCardProps } from '@components/components/ToggleCard/types';

export const ToggleCard = ({
    title,
    subTitle,
    value,
    disabled,
    onToggle,
    toggleDataTestId,
    children,
}: ToggleCardProps) => {
    return (
        <CardContainer>
            <Header>
                <TitleContainer>
                    <Title>{title}</Title>
                    <SubTitleContainer>
                        <SubTitle>{subTitle}</SubTitle>
                    </SubTitleContainer>
                </TitleContainer>
                <Switch
                    disabled={disabled}
                    isChecked={value}
                    onChange={(e) => onToggle(e.target.checked)}
                    colorScheme="primary"
                    label=""
                    labelStyle={{ display: 'none' }}
                    data-testid={toggleDataTestId}
                />
            </Header>
            {children}
        </CardContainer>
    );
};
