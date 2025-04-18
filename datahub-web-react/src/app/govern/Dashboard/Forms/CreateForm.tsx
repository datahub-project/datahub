import React from 'react';
import { useLocation } from 'react-router';

import FormContent from '@app/govern/Dashboard/Forms/FormContent';
import FormFooter from '@app/govern/Dashboard/Forms/FormFooter';
import { ManageFormContextProvider } from '@app/govern/Dashboard/Forms/ManageFormContextProvider';
import { FormMode } from '@app/govern/Dashboard/Forms/formUtils';
import { CreateFormContainer } from '@app/govern/Dashboard/Forms/styledComponents';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

interface Props {
    mode: FormMode;
}

const CreateForm = ({ mode }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const location = useLocation();
    const { inputs, searchAcrossEntities } = location.state || {};
    return (
        <ManageFormContextProvider>
            <CreateFormContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
                <FormContent mode={mode} />
                <FormFooter inputs={inputs} searchAcrossEntities={searchAcrossEntities} />
            </CreateFormContainer>
        </ManageFormContextProvider>
    );
};

export default CreateForm;
