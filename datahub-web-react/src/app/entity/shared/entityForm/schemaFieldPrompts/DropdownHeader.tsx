import { CheckCircle } from '@phosphor-icons/react/dist/csr/CheckCircle';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import translateFieldPath from '@app/entity/dataset/profile/schema/utils/translateFieldPath';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { getNumPromptsCompletedForField } from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';

import { SchemaField } from '@types';

const HeaderWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    font-size: 16px;
    align-items: center;
`;

const PromptsRemainingText = styled.span`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
    font-weight: 400;
`;

const PromptsCompletedText = styled.span`
    font-size: 14px;
    color: ${(props) => props.theme.colors.text};
    font-weight: 600;
`;

const GreenCheckCircle = styled(CheckCircle).attrs({ size: 16, weight: 'fill' })`
    color: ${(props) => props.theme.colors.iconSuccess};
    vertical-align: -2px;
`;

interface Props {
    field: SchemaField;
    numPrompts: number;
    isExpanded: boolean;
}

export default function DropdownHeader({ field, numPrompts, isExpanded }: Props) {
    const { t } = useTranslation('entity.form');
    const { entityData } = useEntityData();
    const { formUrn } = useEntityFormContext();
    const numPromptsCompletedForField = useMemo(
        () => getNumPromptsCompletedForField(field.fieldPath, entityData, formUrn),
        [entityData, field.fieldPath, formUrn],
    );
    const numPromptsRemaining = numPrompts - numPromptsCompletedForField;

    return (
        <HeaderWrapper>
            <span>{t('fieldLabel', { fieldPath: translateFieldPath(field.fieldPath) })}</span>
            {numPromptsRemaining > 0 && (
                <PromptsRemainingText>{t('promptsRemaining', { count: numPromptsRemaining })}</PromptsRemainingText>
            )}
            {numPromptsRemaining === 0 && !isExpanded && (
                <PromptsCompletedText>
                    <GreenCheckCircle /> {t('promptsCompleted', { count: numPrompts })}
                </PromptsCompletedText>
            )}
        </HeaderWrapper>
    );
}
