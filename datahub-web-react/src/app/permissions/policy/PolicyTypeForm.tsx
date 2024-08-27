import React from 'react';
import { Form, Input, Select, Typography } from 'antd';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import { PolicyType } from '../../../types.generated';

type Props = {
    policyType: string;
    setPolicyType: (type: PolicyType) => void;
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
    policyName,
    setPolicyName,
    policyDescription,
    setPolicyDescription,
}: Props) {
    const { t } = useTranslation();
    const updatePolicyName = (name: string) => {
        setPolicyName(name);
    };

    return (
        <TypeForm layout="vertical">
            <Form.Item
                name="policyName"
                labelAlign="right"
                label={<Typography.Text strong>{t('common.name')}</Typography.Text>}
            >
                <Typography.Paragraph>{t('permissions.nameForNewPolicy')}</Typography.Paragraph>
                <Input
                    placeholder={t('placeholder.yourPolicyNamePlaceholder')}
                    data-testid="policy-name"
                    value={policyName}
                    onChange={(event) => updatePolicyName(event.target.value)}
                />
            </Form.Item>
            <Form.Item name="policyType" label={<Typography.Text strong>{t('common.type')}</Typography.Text>}>
                <Typography.Paragraph>{t('permissions.typeOfPolicyToCreate')}</Typography.Paragraph>
                <Select
                    data-testid="policy-type"
                    defaultValue={policyType}
                    onSelect={(value) => setPolicyType(value as PolicyType)}
                >
                    <Select.Option data-testid={t('common.platform')} value={PolicyType.Platform}>
                        {t('common.platform')}
                    </Select.Option>
                    <Select.Option data-testid={t('common.metadata')} value={PolicyType.Metadata}>
                        {t('common.metadata')}
                    </Select.Option>
                </Select>
                <TypeDescriptionParagraph type="secondary">
                    {t('permissions.typeOfPolicyDescriptionPlatform_component')}
                    <br />
                    <br />
                    {t('permissions.typeOfPolicyDescriptionMetadata_component')}
                </TypeDescriptionParagraph>
            </Form.Item>
            <Form.Item
                name="policyDescription"
                labelAlign="right"
                label={<Typography.Text strong>{t('common.description')}</Typography.Text>}
            >
                <Typography.Paragraph> {t('permissions.newPolicyDescription')}</Typography.Paragraph>
                <Input
                    placeholder={t('placeholder.newPolicyDescriptionPlaceHolder')}
                    data-testid="policy-description"
                    value={policyDescription}
                    onChange={(event) => setPolicyDescription(event.target.value)}
                />
            </Form.Item>
        </TypeForm>
    );
}
