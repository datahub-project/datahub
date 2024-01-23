import React from 'react';
import styled from 'styled-components';
import AppLogoLink from '../../../../shared/AppLogoLink';
import EntityNavigation from './EntityNavigation';
import { FormView, useEntityFormContext } from '../EntityFormContext';
import FormViewToggle from './FormViewToggle';
import PromptNavigation from './PromptNavigation';
import BulkVerifyHeader from './BulkVerifyHeader';

const Header = styled.div`
    padding: 12px 24px;
    background-color: black;
    font-size: 24px;
    display: flex;
    align-items: center;
    color: white;
    justify-content: space-between;
`;

const HeaderText = styled.div`
    margin-left: 24px;
`;

const StyledDivider = styled.div`
    display: flex;
    flex-direction: column;
`;

const TitleWrapper = styled.div`
    display: flex;
    align-items: center;
`;

interface Props {
    formUrn: string;
}

export default function FormPageHeader({ formUrn }: Props) {
    const { formView } = useEntityFormContext();

    return (
        <StyledDivider>
            <Header>
                <TitleWrapper>
                    <AppLogoLink />
                    <HeaderText>Complete Documentation Requests</HeaderText>
                </TitleWrapper>
                <FormViewToggle />
            </Header>
            {formView === FormView.BY_ENTITY && <EntityNavigation />}
            {formView === FormView.BY_QUESTION && <PromptNavigation formUrn={formUrn} />}
            {formView === FormView.BULK_VERIFY && <BulkVerifyHeader formUrn={formUrn} />}
        </StyledDivider>
    );
}
