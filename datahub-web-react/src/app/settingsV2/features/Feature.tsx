import { Button, Pill, Switch, Tooltip2 } from '@components';
import { Card, Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { colors } from '@components/theme';

const StyledCard = styled(Card)`
    && {
        border-radius: 12px;
        border: 1px solid ${colors.gray[100]};
        box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
        margin-bottom: 24px;

        .ant-card-body {
            padding: 16px;
        }
    }
`;

const Title = styled.div`
    font-size: 16px;
    font-weight: 700;
    color: ${colors.gray[1700]};
    display: flex;
    align-items: center;
    gap: 8px;
`;

const TitleDescriptionText = styled.div`
    font-size: 14px;
    color: ${colors.gray[1700]};
`;

const SettingTitle = styled.div`
    display: flex;
    align-items: start;
    font-size: 14px;
    flex-direction: column;
    margin-bottom: 24px;
`;

const FeatureRow = styled.div`
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
`;

const FeatureOptionRow = styled.div`
    display: flex;
    justify-content: space-between;

    &:not(:last-child) {
        margin-bottom: 8px;
    }
`;

const StyledDivider = styled(Divider)`
    color: ${colors.gray[100]};
`;

const SettingsOptionRow = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0 16px;

    &:not(:last-child) {
        margin-bottom: 8px;
    }
`;

const DescriptionText = styled.div`
    color: ${colors.gray[1700]};
    font-size: 12px;
`;

const OptionTitle = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14px;
    color: ${colors.gray[1700]};
    font-weight: 600;
`;

export interface FeatureType {
    key: string;
    title: string;
    description: string;
    settings: Array<{
        key: string;
        title: string;
        isAvailable: boolean;
        buttonText: string;
        onClick?: () => void;
    }>;
    options: Array<{
        key: string;
        title: string;
        description: string;
        isAvailable: boolean;
        isDisabled: boolean;
        disabledMessage?: string;
        checked: boolean;
        onChange?: (checked: boolean) => void;
    }>;
    isNew: boolean;
    learnMoreLink?: string;
}

export const Feature = ({ key, title, description, settings, options, isNew, learnMoreLink }: FeatureType) => (
    <StyledCard key={key}>
        <FeatureRow>
            <SettingTitle>
                <Title>
                    {title} {isNew && <Pill color="blue" size="sm" label="New!" />}
                </Title>
                <TitleDescriptionText>{description}</TitleDescriptionText>
            </SettingTitle>
            <div>
                {learnMoreLink && (
                    <Button
                        variant="text"
                        onClick={() => window.open(learnMoreLink, '_blank')}
                        icon={{ icon: 'ArrowRight', source: 'phosphor' }}
                        iconPosition="right"
                    >
                        Learn more
                    </Button>
                )}
            </div>
        </FeatureRow>
        <StyledCard>
            {options.map((option, index) => (
                <>
                    <FeatureOptionRow key={option.key}>
                        <span>
                            <OptionTitle>
                                <span>{option.title}</span>
                                {!option.isAvailable && (
                                    <Pill color="violet" size="sm" label="Only available on DataHub Cloud" />
                                )}
                            </OptionTitle>
                            <div>
                                <DescriptionText>{option.description}</DescriptionText>
                            </div>
                        </span>
                        {option.disabledMessage ? (
                            <Tooltip2
                                title={option.disabledMessage}
                                placement="top"
                                showArrow
                                mouseEnterDelay={0.1}
                                mouseLeaveDelay={0.1}
                            >
                                <span style={{ cursor: 'default' }}>
                                    <Switch
                                        label=""
                                        checked={option.checked}
                                        onChange={(e) => (option.onChange ? option.onChange(e.target.checked) : null)}
                                        disabled={!option.isAvailable || option.isDisabled}
                                    />
                                </span>
                            </Tooltip2>
                        ) : (
                            <Switch
                                label=""
                                checked={option.checked}
                                onChange={(e) => (option.onChange ? option.onChange(e.target.checked) : null)}
                                disabled={!option.isAvailable || option.isDisabled}
                            />
                        )}
                    </FeatureOptionRow>
                    {index !== options.length - 1 && <StyledDivider />}
                </>
            ))}
        </StyledCard>
        {settings.map((option) => (
            <>
                <SettingsOptionRow key={option.key}>
                    <span>
                        <OptionTitle>
                            <span>{option.title}</span>
                            <Pill color="violet" size="sm" label="DataHub Cloud" />
                        </OptionTitle>
                    </span>
                    <Tooltip2
                        title={option.isAvailable ? '' : 'Only available on DataHub Cloud'}
                        placement="left"
                        showArrow
                        mouseEnterDelay={0.1}
                        mouseLeaveDelay={0.1}
                    >
                        <span>
                            <Button onClick={option.onClick} disabled={!option.isAvailable}>
                                {option.buttonText}
                            </Button>
                        </span>
                    </Tooltip2>
                </SettingsOptionRow>
            </>
        ))}
    </StyledCard>
);
