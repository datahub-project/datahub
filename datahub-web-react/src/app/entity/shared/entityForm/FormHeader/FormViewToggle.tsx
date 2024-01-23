import { SwapOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FormView, useEntityFormContext } from '../EntityFormContext';
import { WhiteButton } from '../../../../shared/components';
import { useEntityData } from '../../EntityContext';
import { getBulkByQuestionPrompts } from '../../containers/profile/sidebar/FormInfo/utils';
import { FORM_QUESTION_VIEW_BUTTON } from '../../../../onboarding/config/FormOnboardingConfig';

const buttonStypes = `
padding: 12px 32px;
display: flex;
align-items: center;
margin-right: 44px;
font-size: 14px;
`;

const ToggleButton = styled(WhiteButton)`
    ${buttonStypes}
    .anticon {
        line-height: 0;
    }
`;

const ReturnButton = styled(Button)`
    ${buttonStypes}
    color: white;

    &:hover {
        color: white;
    }
`;

const ButtonText = styled.span`
    margin-left: 8px;
`;

export default function FormViewToggle() {
    const { formView, setFormView, refetchNumReadyForVerification, formUrn, setSelectedPromptId, setSelectedEntities } =
        useEntityFormContext();
    const { entityData } = useEntityData();
    const prompts = getBulkByQuestionPrompts(formUrn, entityData);

    function toggleFormView() {
        if (formView === FormView.BY_ENTITY) {
            setFormView(FormView.BY_QUESTION);
            refetchNumReadyForVerification();
        } else if (formView === FormView.BY_QUESTION) {
            setFormView(FormView.BY_ENTITY);
        }
    }

    function returnToQuestions() {
        setFormView(FormView.BY_QUESTION);
        setSelectedEntities([]);
        if (prompts.length) {
            setSelectedPromptId(prompts[0].id);
        }
    }

    if (formView === FormView.BULK_VERIFY) {
        return (
            <ReturnButton type="text" onClick={returnToQuestions}>
                Return to Questions
            </ReturnButton>
        );
    }

    return (
        <ToggleButton
            onClick={toggleFormView}
            id={formView === FormView.BY_ENTITY ? FORM_QUESTION_VIEW_BUTTON : undefined}
        >
            <SwapOutlined />
            <ButtonText>Complete by {formView === FormView.BY_ENTITY ? 'Question' : 'Entity'}</ButtonText>
        </ToggleButton>
    );
}
