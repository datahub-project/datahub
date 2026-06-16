import { Form, Input, Select, Typography } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useIsDenyPoliciesEnabled } from '@app/shared/hooks/useIsDenyPoliciesEnabled';

import { PolicyEffect, PolicyType } from '@types';

type Props = {
    policyType: string;
    setPolicyType: (type: PolicyType) => void;
    policyEffect: PolicyEffect;
    setPolicyEffect: (effect: PolicyEffect) => void;
    policyName: string;
    setPolicyName: (name: string) => void;
    policyDescription: string;
    setPolicyDescription: (description: string) => void;
};

const TypeForm = styled(Form)`
    margin: 12px;
    margin-top: 36px;
    > div {
        margin-bottom: 28px;
    }
`;

const TypeDescriptionParagraph = styled(Typography.Paragraph)`
    margin-top: 12px;
`;

export default function PolicyTypeForm({
    policyType,
    setPolicyType,
    policyEffect,
    setPolicyEffect,
    policyName,
    setPolicyName,
    policyDescription,
    setPolicyDescription,
}: Props) {
    const { t } = useTranslation('settings.permissions');
    const denyPoliciesEnabled = useIsDenyPoliciesEnabled();

    const updatePolicyName = (name: string) => {
        setPolicyName(name);
    };

    return (
        <TypeForm layout="vertical">
            <Form.Item
                name="policyName"
                labelAlign="right"
                label={<Typography.Text strong>{t('column.name')}</Typography.Text>}
            >
                <Typography.Paragraph>{t('typeForm.nameDescription')}</Typography.Paragraph>
                <Input
                    placeholder={t('typeForm.namePlaceholder')}
                    data-testid="policy-name"
                    value={policyName}
                    onChange={(event) => updatePolicyName(event.target.value)}
                />
            </Form.Item>
            <Form.Item name="policyType" label={<Typography.Text strong>{t('column.type')}</Typography.Text>}>
                <Typography.Paragraph>{t('typeForm.typeDescription')}</Typography.Paragraph>
                <Select
                    data-testid="policy-type"
                    defaultValue={policyType}
                    onSelect={(value) => setPolicyType(value as PolicyType)}
                >
                    <Select.Option data-testid="platform" value={PolicyType.Platform}>
                        {t('typeForm.platformOption')}
                    </Select.Option>
                    <Select.Option data-testid="metadata" value={PolicyType.Metadata}>
                        {t('typeForm.metadataOption')}
                    </Select.Option>
                </Select>
                <TypeDescriptionParagraph type="secondary">
                    <Trans t={t} i18nKey="typeForm.platformDescription" components={{ bold: <b /> }} />
                    <br />
                    <br />
                    <Trans t={t} i18nKey="typeForm.metadataDescription" components={{ bold: <b /> }} />
                </TypeDescriptionParagraph>
            </Form.Item>
            {denyPoliciesEnabled && (
                <Form.Item
                    name="policyEffect"
                    label={<Typography.Text strong>{t('typeForm.effectLabel')}</Typography.Text>}
                >
                    <Typography.Paragraph>{t('typeForm.effectDescription')}</Typography.Paragraph>
                    <Select
                        data-testid="policy-effect"
                        value={policyEffect}
                        onSelect={(value) => setPolicyEffect(value as PolicyEffect)}
                    >
                        <Select.Option data-testid="allow" value={PolicyEffect.Allow}>
                            {t('typeForm.effectAllowOption')}
                        </Select.Option>
                        <Select.Option data-testid="deny" value={PolicyEffect.Deny}>
                            {t('typeForm.effectDenyOption')}
                        </Select.Option>
                    </Select>
                    <TypeDescriptionParagraph type="secondary">
                        <Trans t={t} i18nKey="typeForm.effectDescriptionDetail" components={{ bold: <b /> }} />
                    </TypeDescriptionParagraph>
                </Form.Item>
            )}
            <Form.Item
                name="policyDescription"
                labelAlign="right"
                label={<Typography.Text strong>{t('column.description')}</Typography.Text>}
            >
                <Typography.Paragraph>{t('typeForm.descriptionDescription')}</Typography.Paragraph>
                <Input
                    placeholder={t('typeForm.descriptionPlaceholder')}
                    data-testid="policy-description"
                    value={policyDescription}
                    onChange={(event) => setPolicyDescription(event.target.value)}
                />
            </Form.Item>
        </TypeForm>
    );
}
