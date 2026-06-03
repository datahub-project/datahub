import { Icon, Text } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Collapse } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import {
    CollapseHeader,
    RowContainer,
    StyledCollapse,
    StyledLabel,
} from '@app/govern/structuredProperties/styledComponents';
import { StructuredPropertyEntity } from '@src/types.generated';

interface Props {
    propEntity: StructuredPropertyEntity;
}

const ViewAdvancedOptions = ({ propEntity }: Props) => {
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
                        <Text weight="bold" color="gray" size="lg">
                            {t('advancedOptions.title')}
                        </Text>
                    </CollapseHeader>
                }
                forceRender
            >
                {propEntity && (
                    <RowContainer>
                        <StyledLabel>{t('advancedOptions.qualifiedName')}</StyledLabel>
                        <Text color="gray"> {propEntity?.definition?.qualifiedName}</Text>
                    </RowContainer>
                )}
            </Collapse.Panel>
        </StyledCollapse>
    );
};

export default ViewAdvancedOptions;
