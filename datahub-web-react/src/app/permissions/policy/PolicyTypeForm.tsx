import { Input, SimpleSelect, Text } from '@components';
import { Form } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { PolicyType } from '@types';

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
        margin-bottom: 16px;
    }
`;

const TypeDescriptionParagraph = styled.div`
    margin-top: 12px;
    margin-bottom: 12px;
`;

const SelectWrapper = styled.div`
    width: 100%;
    margin-top: 8px;
    margin-bottom: 8px;
`;

const InputWrapper = styled.div`
    margin-top: 8px;
`;

export default function PolicyTypeForm({
    policyType,
    setPolicyType,
    policyName,
    setPolicyName,
    policyDescription,
    setPolicyDescription,
}: Props) {
    const { t } = useTranslation('settings.permissions');

    const updatePolicyName = (name: string) => {
        setPolicyName(name);
    };

    return (
        <TypeForm layout="vertical">
            <Form.Item name="policyName" labelAlign="right" label={<Text>{t('column.name')}</Text>}>
                <Text color="textSecondary">{t('typeForm.nameDescription')}</Text>
                <InputWrapper>
                    <Input
                        placeholder={t('typeForm.namePlaceholder')}
                        value={policyName}
                        onChange={(event) => updatePolicyName(event.target.value)}
                        inputTestId="policy-name"
                    />
                </InputWrapper>
            </Form.Item>
            <Form.Item name="policyType" label={<Text>{t('column.type')}</Text>}>
                <Text color="textSecondary">{t('typeForm.typeDescription')}</Text>
                <SelectWrapper>
                    <SimpleSelect
                        options={[
                            { value: PolicyType.Platform, label: t('typeForm.platformOption') },
                            { value: PolicyType.Metadata, label: t('typeForm.metadataOption') },
                        ]}
                        values={policyType ? [policyType] : []}
                        onUpdate={(values) => setPolicyType(values[0] as PolicyType)}
                        isMultiSelect={false}
                        dataTestId="policy-type"
                        width="full"
                        showClear={false}
                    />
                </SelectWrapper>
                <TypeDescriptionParagraph>
                    <Text color="textSecondary" size="sm">
                        <Trans t={t} i18nKey="typeForm.platformDescription" components={{ bold: <b /> }} />
                        <br />
                        <br />
                        <Trans t={t} i18nKey="typeForm.metadataDescription" components={{ bold: <b /> }} />
                    </Text>
                </TypeDescriptionParagraph>
            </Form.Item>
            <Form.Item name="policyDescription" labelAlign="right" label={<Text>{t('column.description')}</Text>}>
                <Text color="textSecondary">{t('typeForm.descriptionDescription')}</Text>
                <InputWrapper>
                    <Input
                        placeholder={t('typeForm.descriptionPlaceholder')}
                        value={policyDescription}
                        onChange={(event) => setPolicyDescription(event.target.value)}
                        inputTestId="policy-description"
                    />
                </InputWrapper>
            </Form.Item>
        </TypeForm>
    );
}
