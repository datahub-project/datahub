import { Tooltip } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import useGetPromptInfo from '@app/entity/shared/containers/profile/sidebar/FormInfo/useGetPromptInfo';
import useIsUserAssigned from '@app/entity/shared/containers/profile/sidebar/FormInfo/useIsUserAssigned';
import {
    isVerificationComplete,
    shouldShowVerificationInfo,
} from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import FormRequestedBy from '@app/entity/shared/entityForm/FormSelectionModal/FormRequestedBy';
import { WhiteButton } from '@app/shared/components';

import { FormAssociation } from '@types';

const FormItemWrapper = styled.div`
    display: flex;
    padding: 16px;
    justify-content: space-between;
`;

const FormName = styled.div`
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 4px;
`;

const FormAssigner = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.text};
    margin-top: -4px;
    margin-bottom: 4px;
`;

const OptionalText = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-weight: normal;
`;

const CompleteWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const FormInfoWrapper = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.text};
    font-weight: 600;
`;

interface Props {
    formAssociation: FormAssociation;
    selectFormUrn: (urn: string) => void;
}

export default function FormItem({ formAssociation, selectFormUrn }: Props) {
    const { t } = useTranslation('entity.form');
    const { t: tc } = useTranslation('common.actions');
    const { entityData } = useEntityData();
    const { form } = formAssociation;
    const { numRequiredPromptsRemaining, numOptionalPromptsRemaining } = useGetPromptInfo(form.urn);
    const allRequiredPromptsAreComplete = numRequiredPromptsRemaining === 0;
    const showVerificationInfo = shouldShowVerificationInfo(entityData, form.urn);
    const isComplete = showVerificationInfo
        ? isVerificationComplete(entityData, form.urn)
        : allRequiredPromptsAreComplete;
    const isUserAssigned = useIsUserAssigned(form.urn);
    const owners = form.ownership?.owners;

    return (
        <FormItemWrapper>
            <div>
                <FormName>{form.info.name}</FormName>
                {owners && owners.length > 0 && (
                    <FormAssigner>
                        <FormRequestedBy owners={owners} />
                    </FormAssigner>
                )}
                <FormInfoWrapper>
                    {isComplete && (
                        <CompleteWrapper>
                            {showVerificationInfo ? <>{t('statusVerified')}</> : <>{t('statusComplete')}</>}
                        </CompleteWrapper>
                    )}
                    {!isComplete && (
                        <div>{t('requiredResponsesRemaining', { count: numRequiredPromptsRemaining })}</div>
                    )}
                    {numOptionalPromptsRemaining > 0 && (
                        <OptionalText>
                            {t('optionalResponsesRemaining', { count: numOptionalPromptsRemaining })}
                        </OptionalText>
                    )}
                </FormInfoWrapper>
            </div>
            <Tooltip title={!isUserAssigned ? t('notAssignedTooltip') : undefined}>
                <WhiteButton type="primary" onClick={() => selectFormUrn(form.urn)} disabled={!isUserAssigned}>
                    {isComplete && tc('view')}
                    {!isComplete && <>{showVerificationInfo ? t('verify') : t('statusComplete')}</>}
                </WhiteButton>
            </Tooltip>
        </FormItemWrapper>
    );
}
