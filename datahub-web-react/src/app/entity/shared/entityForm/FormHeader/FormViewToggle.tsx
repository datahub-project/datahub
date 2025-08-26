import { SwapOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import * as QueryString from 'query-string';
import React from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { FORM_QUESTION_VIEW_BUTTON } from '@app/onboarding/config/FormOnboardingConfig';
import { WhiteButton } from '@app/shared/components';
import analytics, { EventType } from '@src/app/analytics';

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
    const history = useHistory();
    const location = useLocation();
    const {
        form: { formView, setFormView },
        prompt: { prompts, setSelectedPromptId },
        entity: { setSelectedEntities, refetch },
    } = useEntityFormContext();

    function toggleFormView() {
        if (formView === FormView.BY_ENTITY) {
            analytics.event({ type: EventType.FormViewToggle, formView: 'byQuestion' });
            setFormView(FormView.BY_QUESTION);
            const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
            history.push({ search: QueryString.stringify({ ...params, page: 0 }) });
        } else if (formView === FormView.BY_QUESTION) {
            analytics.event({ type: EventType.FormViewToggle, formView: 'byAsset' });
            refetch();
            setFormView(FormView.BY_ENTITY);
        }
    }

    function returnToQuestions() {
        setFormView(FormView.BY_QUESTION);
        setSelectedEntities([]);
        if (prompts && prompts.length) setSelectedPromptId(prompts[0].id);
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
            <ButtonText>Complete by {formView === FormView.BY_ENTITY ? 'Question' : 'Asset'}</ButtonText>
        </ToggleButton>
    );
}
