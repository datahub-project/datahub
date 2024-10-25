import React from 'react';
import { useLocation } from 'react-router';
import FormContent from './FormContent';
import FormFooter from './FormFooter';
import { FormMode } from './formUtils';
import { ManageFormContextProvider } from './ManageFormContextProvider';
import { CreateFormContainer } from './styledComponents';

interface Props {
    mode: FormMode;
}

const CreateForm = ({ mode }: Props) => {
    const location = useLocation();
    const { inputs, searchAcrossEntities } = location.state || {};
    return (
        <ManageFormContextProvider>
            <CreateFormContainer>
                <FormContent mode={mode} />
                <FormFooter inputs={inputs} searchAcrossEntities={searchAcrossEntities} />
            </CreateFormContainer>
        </ManageFormContextProvider>
    );
};

export default CreateForm;
