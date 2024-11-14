import React from 'react';
import { useLocation } from 'react-router';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import FormContent from './FormContent';
import FormFooter from './FormFooter';
import { FormMode } from './formUtils';
import { ManageFormContextProvider } from './ManageFormContextProvider';
import { CreateFormContainer } from './styledComponents';

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
