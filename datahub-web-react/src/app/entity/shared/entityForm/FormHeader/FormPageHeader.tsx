import React from 'react';
import styled from 'styled-components';

import BulkVerifyHeader from '@app/entity/shared/entityForm/BulkVerify/BulkVerifyHeader';
import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import EntityNavigation from '@app/entity/shared/entityForm/FormHeader/EntityNavigation';
import FormViewToggle from '@app/entity/shared/entityForm/FormHeader/FormViewToggle';
import PromptNavigation from '@app/entity/shared/entityForm/FormHeader/PromptNavigation';
import AppLogoLink from '@app/shared/AppLogoLink';
import { PageTitle, colors } from '@src/alchemy-components';

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
