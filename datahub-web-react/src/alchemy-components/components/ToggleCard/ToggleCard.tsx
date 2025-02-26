import React from 'react';
import { ToggleCardProps } from './types';
import { CardContainer, SubTitle, SubTitleContainer, Title, TitleContainer } from '../Card/components';
import { Switch } from '../Switch';
import { Header } from './components';

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
                    colorScheme="violet"
                    label=""
                    labelStyle={{ display: 'none' }}
                    data-testid={toggleDataTestId}
                />
            </Header>
            {children}
        </CardContainer>
    );
};
