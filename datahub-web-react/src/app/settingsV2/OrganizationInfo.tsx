import { Button, Input, Text } from '@components';
import { Form } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useGlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';
import { ToastType, showToastMessage } from '@app/sharedV2/toastMessageUtils';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { useUpdateOrganizationDisplayPreferencesMutation } from '@src/graphql/settings.generated';

const OrganizationSection = styled.div`
    display: grid;
    gap: 20px;
    margin: 32px 0;
`;

const SectionHeader = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;

    p {
        line-height: normal;
    }
`;

const ButtonsContainer = styled.div`
    display: flex;
    gap: 16px;
    justify-self: end;
`;

const OrganizationInfo = () => {
    const { globalSettings, refetch } = useGlobalSettingsContext();
    const [updateDisplayPreferences] = useUpdateOrganizationDisplayPreferencesMutation();
    const [form] = Form.useForm();
    const isThemeV2 = useIsThemeV2();
    const [orgNameValue, setOrgNameValue] = useState('');

    useEffect(() => {
        const orgName = globalSettings?.visualSettings?.customOrgName || '';
        form.setFieldsValue({
            customOrgName: orgName,
            customLogoUrl: globalSettings?.visualSettings?.customLogoUrl || undefined,
        });
        setOrgNameValue(orgName);
    }, [globalSettings, form]);

    const handleSave = () => {
        updateDisplayPreferences({
            variables: {
                input: form.getFieldsValue(),
            },
        })
            .then(() => {
                showToastMessage(ToastType.SUCCESS, 'Setting Updated', 3);
                refetch();
            })
            .catch(() => {
                showToastMessage(ToastType.ERROR, 'Failed to update setting', 3);
            });
    };

    const handleCancel = () => {
        form.setFieldsValue({
            customOrgName: globalSettings?.visualSettings?.customOrgName || undefined,
            customLogoUrl: globalSettings?.visualSettings?.customLogoUrl || undefined,
        });
    };

    return (
        <OrganizationSection>
            <SectionHeader>
                <Text size="xl" weight="bold">
                    Organization Information
                </Text>
                <Text color="gray">Personalize your experience</Text>
            </SectionHeader>
            <Form form={form}>
                <Form.Item name="customOrgName">
                    <Input
                        label="Name"
                        placeholder="Organization Name"
                        maxLength={25}
                        value={orgNameValue}
                        setValue={(value) => {
                            setOrgNameValue(value);
                            form.setFieldValue('customOrgName', value);
                        }}
                        helperText={orgNameValue.length > 0 ? `${orgNameValue.length}/25 characters` : ''}
                    />
                </Form.Item>
                <Form.Item name="customLogoUrl">
                    <Input label="Image URL" placeholder="https://" />
                </Form.Item>
            </Form>
            <ButtonsContainer>
                <Button onClick={handleCancel} variant="text" color={isThemeV2 ? 'violet' : 'blue'}>
                    Cancel
                </Button>
                <Button onClick={handleSave} color={isThemeV2 ? 'violet' : 'blue'}>
                    Save
                </Button>
            </ButtonsContainer>
        </OrganizationSection>
    );
};

export default OrganizationInfo;
