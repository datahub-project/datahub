import { Form, Input, Select, Typography } from 'antd';
import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ViewTypeLabel } from '@app/entity/view/ViewTypeLabel';
import { ViewDefinitionBuilder } from '@app/entity/view/builder/ViewDefinitionBuilder';
import { ViewBuilderMode } from '@app/entity/view/builder/types';
import { ViewBuilderState } from '@app/entity/view/types';

import { DataHubViewType } from '@types';

const StyledFormItem = styled(Form.Item)`
    margin-bottom: 8px;
`;

type Props = {
    urn?: string;
    mode: ViewBuilderMode;
    state: ViewBuilderState;
    updateState: (newState: ViewBuilderState) => void;
};

export const ViewBuilderForm = ({ urn, mode, state, updateState }: Props) => {
    const { t } = useTranslation('entity.views');
    const theme = useTheme();
    const userContext = useUserContext();
    const [form] = Form.useForm();

    useEffect(() => {
        form.setFieldsValue(state);
    }, [state, form]);

    const setName = (name: string) => {
        updateState({
            ...state,
            name,
        });
    };

    const setDescription = (description: string) => {
        updateState({
            ...state,
            description,
        });
    };

    const setViewType = (viewType: DataHubViewType) => {
        updateState({
            ...state,
            viewType,
        });
    };

    const canManageGlobalViews = userContext?.platformPrivileges?.manageGlobalViews || false;
    const isEditing = urn !== undefined;

    return (
        <span data-testid="view-builder-form">
            <Form form={form} initialValues={state} layout="vertical">
                <StyledFormItem label={<Typography.Text strong>{t('viewForm.nameLabel')}</Typography.Text>}>
                    <Typography.Paragraph>{t('viewForm.nameHelp')}</Typography.Paragraph>
                    <Form.Item
                        name="name"
                        rules={[
                            {
                                required: true,
                                message: t('viewForm.nameRequired'),
                            },
                            { whitespace: true },
                            { min: 1, max: 50 },
                        ]}
                        hasFeedback
                    >
                        <Input
                            data-testid="view-name-input"
                            placeholder={t('viewForm.namePlaceholder')}
                            onChange={(event) => setName(event.target.value)}
                            disabled={mode === ViewBuilderMode.PREVIEW}
                        />
                    </Form.Item>
                </StyledFormItem>
                <StyledFormItem label={<Typography.Text strong>{t('viewForm.descriptionLabel')}</Typography.Text>}>
                    <Typography.Paragraph>{t('viewForm.descriptionHelp')}</Typography.Paragraph>
                    <Form.Item name="description" rules={[{ whitespace: true }, { min: 1, max: 500 }]} hasFeedback>
                        <Input.TextArea
                            data-testid="view-description-input"
                            placeholder={t('viewForm.descriptionPlaceholderLegacy')}
                            onChange={(event) => setDescription(event.target.value)}
                            disabled={mode === ViewBuilderMode.PREVIEW}
                        />
                    </Form.Item>
                </StyledFormItem>
                <StyledFormItem label={<Typography.Text strong>{t('viewForm.typeLabel')}</Typography.Text>}>
                    <Typography.Paragraph>{t('viewForm.typeHelp')}</Typography.Paragraph>
                    <Form.Item name="viewType">
                        <Select
                            onSelect={(value) => setViewType(value as DataHubViewType)}
                            disabled={!canManageGlobalViews || isEditing || mode === ViewBuilderMode.PREVIEW}
                        >
                            <Select.Option value={DataHubViewType.Personal}>
                                <ViewTypeLabel type={DataHubViewType.Personal} color={theme.colors.text} />
                            </Select.Option>
                            <Select.Option value={DataHubViewType.Global}>
                                <ViewTypeLabel type={DataHubViewType.Global} color={theme.colors.text} />
                            </Select.Option>
                        </Select>
                    </Form.Item>
                </StyledFormItem>
                <StyledFormItem
                    label={<Typography.Text strong>{t('viewForm.filtersLabel')}</Typography.Text>}
                    style={{ marginBottom: 8 }}
                >
                    <Typography.Paragraph>{t('viewForm.filtersHelp')}</Typography.Paragraph>
                </StyledFormItem>
            </Form>
            <ViewDefinitionBuilder mode={mode} state={state} updateState={updateState} />
        </span>
    );
};
