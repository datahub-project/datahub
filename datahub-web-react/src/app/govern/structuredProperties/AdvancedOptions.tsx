import { Icon, Input, Text, Tooltip } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import { Collapse, Form } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import {
    CollapseHeader,
    FlexContainer,
    InputLabel,
    StyledCollapse,
} from '@app/govern/structuredProperties/styledComponents';

interface Props {
    isEditMode: boolean;
}

const AdvancedOptions = ({ isEditMode }: Props) => {
    const { t } = useTranslation('governance.structured-properties');

    return (
        <StyledCollapse
            ghost
            expandIcon={({ isActive }) => (
                <Icon icon={CaretRight} color="gray" size="4xl" rotate={isActive ? '90' : '0'} />
            )}
            expandIconPosition="end"
            defaultActiveKey={[]}
        >
            <Collapse.Panel
                key={1}
                header={
                    <CollapseHeader>
                        <Text weight="bold" color="gray">
                            {t('advancedOptions.title')}
                        </Text>
                    </CollapseHeader>
                }
                forceRender
            >
                <InputLabel>
                    <FlexContainer>
                        {t('advancedOptions.qualifiedName')}
                        <Tooltip title={t('advancedOptions.qualifiedNameTooltip')} showArrow={false}>
                            <Icon icon={Info} color="violet" size="lg" />
                        </Tooltip>
                    </FlexContainer>
                </InputLabel>
                <Tooltip title={isEditMode && t('advancedOptions.qualifiedNameDisabledTooltip')} showArrow={false}>
                    <Form.Item
                        name="qualifiedName"
                        rules={[
                            {
                                pattern: /^[^\s]*$/,
                                whitespace: true,
                                message: t('advancedOptions.qualifiedNameError'),
                            },
                        ]}
                    >
                        <Input
                            label=""
                            placeholder={t('advancedOptions.qualifiedNamePlaceholder')}
                            isDisabled={isEditMode}
                        />
                    </Form.Item>
                </Tooltip>
            </Collapse.Panel>
        </StyledCollapse>
    );
};

export default AdvancedOptions;
