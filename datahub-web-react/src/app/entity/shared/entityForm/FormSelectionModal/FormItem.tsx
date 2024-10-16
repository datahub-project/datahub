import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FormAssociation } from '../../../../../types.generated';
import useGetPromptInfo from '../../containers/profile/sidebar/FormInfo/useGetPromptInfo';
import { useEntityData } from '../../EntityContext';
import { isVerificationComplete, shouldShowVerificationInfo } from '../../containers/profile/sidebar/FormInfo/utils';
import { pluralize } from '../../../../shared/textUtil';
import { WhiteButton } from '../../../../shared/components';
import { ANTD_GRAY_V2 } from '../../constants';
import useIsUserAssigned from '../../containers/profile/sidebar/FormInfo/useIsUserAssigned';
import FormRequestedBy from './FormRequestedBy';

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
    color: #373d44;
    margin-top: -4px;
    margin-bottom: 4px;
`;

const OptionalText = styled.div`
    color: ${ANTD_GRAY_V2[8]};
    font-weight: normal;
`;

const CompleteWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const FormInfoWrapper = styled.div`
    font-size: 12px;
    color: #373d44;
    font-weight: 600;
`;

interface Props {
    formAssociation: FormAssociation;
    selectFormUrn: (urn: string) => void;
}

export default function FormItem({ formAssociation, selectFormUrn }: Props) {
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
                        <CompleteWrapper>{showVerificationInfo ? <>Verified</> : <>Complete</>}</CompleteWrapper>
                    )}
                    {!isComplete && (
                        <div>
                            {numRequiredPromptsRemaining} required {pluralize(numRequiredPromptsRemaining, 'response')}{' '}
                            remaining
                        </div>
                    )}
                    {numOptionalPromptsRemaining > 0 && (
                        <OptionalText>
                            {numOptionalPromptsRemaining} optional {pluralize(numOptionalPromptsRemaining, 'response')}{' '}
                            remaining
                        </OptionalText>
                    )}
                </FormInfoWrapper>
            </div>
            <Tooltip title={!isUserAssigned ? 'You are not assigned to view or edit this form' : undefined}>
                <WhiteButton type="primary" onClick={() => selectFormUrn(form.urn)} disabled={!isUserAssigned}>
                    {isComplete && 'View'}
                    {!isComplete && <>{showVerificationInfo ? 'Verify' : 'Complete'}</>}
                </WhiteButton>
            </Tooltip>
        </FormItemWrapper>
    );
}
