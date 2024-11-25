import React from 'react';

import styled from 'styled-components';

import { Divider, Typography, Switch, Card, Button, Tooltip } from 'antd';
import { ArrowRightOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../entity/shared/constants';

const Title = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
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

const SettingsOptionRow = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0 16px;

    &:not(:last-child) {
        margin-bottom: 8px;
    }
`;

const DescriptionText = styled(Typography.Text)`
    color: ${ANTD_GRAY[7]};
    font-size: 11px;
`;

const SettingTitle = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14px;
    margin-bottom: 4px;
`;

const OptionTitle = styled(Typography.Text)`
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
`;

const learnMoreLinkStyle = {
    flex: 1,
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    color: '#1890FF',
    fontSize: '12px',
    cursor: 'pointer',
};

const NewTag = styled.div`
    padding: 4px 8px;

    border-radius: 24px;
    background: #f1fbfe;

    color: #09739a;
    font-size: 12px;
`;

const DataHubOnlyTag = styled.div`
    padding: 2px 8px;

    border-radius: 24px;
    background: #c9fff2;

    color: #50a494;
    font-size: 12px;
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
    <Card style={{ marginBottom: '1rem' }} key={key}>
        <FeatureRow>
            <div style={{ flex: 1 }}>
                <SettingTitle>
                    <Title level={5} style={{ marginBottom: 0 }}>
                        {title}
                    </Title>
                    {isNew && <NewTag>New!</NewTag>}
                </SettingTitle>
                <div>
                    <Typography.Paragraph type="secondary">{description}</Typography.Paragraph>
                </div>
            </div>
            <div>
                {learnMoreLink && (
                    <a href={learnMoreLink} target="_blank" style={learnMoreLinkStyle} rel="noreferrer">
                        Learn more <ArrowRightOutlined />
                    </a>
                )}
            </div>
        </FeatureRow>
        <Divider style={{ margin: `8px 0 24px 0` }} />
        <Card style={{ margin: `16px auto` }}>
            {options.map((option, index) => (
                <>
                    <FeatureOptionRow key={option.key}>
                        <span>
                            <OptionTitle>
                                <span>{option.title}</span>
                                {!option.isAvailable && (
                                    <DataHubOnlyTag>Only available on DataHub Cloud</DataHubOnlyTag>
                                )}
                            </OptionTitle>
                            <div>
                                <DescriptionText>{option.description}</DescriptionText>
                            </div>
                        </span>
                        <Tooltip title={option.disabledMessage}>
                            <Switch
                                checked={option.checked}
                                onChange={(checked) => (option.onChange ? option.onChange(checked) : null)}
                                disabled={!option.isAvailable || option.isDisabled}
                            />
                        </Tooltip>
                    </FeatureOptionRow>
                    {index !== options.length - 1 && <Divider />}
                </>
            ))}
        </Card>
        {settings.map((option) => (
            <>
                <SettingsOptionRow key={option.key}>
                    <span>
                        <OptionTitle>
                            <span>{option.title}</span>
                            <DataHubOnlyTag>Only available on DataHub Cloud</DataHubOnlyTag>
                        </OptionTitle>
                    </span>
                    <Tooltip title={option.isAvailable ? '' : 'Only available on DataHub Cloud'}>
                        <Button onClick={option.onClick} disabled={!option.isAvailable}>
                            {option.buttonText}
                        </Button>
                    </Tooltip>
                </SettingsOptionRow>
            </>
        ))}
    </Card>
);
