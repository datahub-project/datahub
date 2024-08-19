import React from 'react';
import FormContent from './FormContent';
import FormFooter from './FormFooter';
import { FormMode } from './formUtils';
import { ManageFormContextProvider } from './ManageFormContextProvider';
import { CreateFormContainer } from './styledComponents';

interface Props {
    mode: FormMode;
}

const CreateForm = ({ mode }: Props) => {
    return (
        <ManageFormContextProvider>
            <CreateFormContainer>
                <FormContent mode={mode} />
                <FormFooter />
            </CreateFormContainer>
        </ManageFormContextProvider>
    );
};

export default CreateForm;
