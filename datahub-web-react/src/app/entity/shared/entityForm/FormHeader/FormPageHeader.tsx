import React from 'react';
import styled from 'styled-components';
import { colors, PageTitle } from '@src/alchemy-components';
import AppLogoLink from '../../../../shared/AppLogoLink';
import EntityNavigation from './EntityNavigation';
import { FormView, useEntityFormContext } from '../EntityFormContext';
import FormViewToggle from './FormViewToggle';
import PromptNavigation from './PromptNavigation';
import BulkVerifyHeader from '../BulkVerify/BulkVerifyHeader';

const Header = styled.div`
    padding: 12px 24px;
    font-size: 24px;
    display: flex;
    align-items: center;
    color: ${colors.gray[600]};
    justify-content: space-between;
    border-bottom: 1px solid ${colors.gray[100]};
`;

const StyledDivider = styled.div`
    display: flex;
    flex-direction: column;
`;

const TitleWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 28px;
`;

export default function FormPageHeader() {
    const {
        form: { formView },
    } = useEntityFormContext();

    return (
        <StyledDivider>
            <Header>
                <TitleWrapper>
                    <AppLogoLink />
                    <PageTitle
                        title="Complete Compliance Tasks"
                        subTitle="Provide the required compliance information for your assets"
                    />
                </TitleWrapper>
                <FormViewToggle />
            </Header>
            {formView === FormView.BY_ENTITY && <EntityNavigation />}
            {formView === FormView.BY_QUESTION && <PromptNavigation />}
            {formView === FormView.BULK_VERIFY && <BulkVerifyHeader />}
        </StyledDivider>
    );
}
