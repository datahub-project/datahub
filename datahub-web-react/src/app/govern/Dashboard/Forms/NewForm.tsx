import React from 'react';
import { MainFormContainer, NewFormContainer } from './styledComponents';
import FormLeftSection from './FormLeftSection';
import FormBody from './FormBody';
import FormFooter from './FormFooter';
import { ManageFormContextProvider } from './ManageFormContextProvider';
import { FormMode } from './formUtils';

interface Props {
    mode: FormMode;
}

const NewForm = ({ mode }: Props) => {
    return (
        <ManageFormContextProvider>
            <NewFormContainer>
                <FormLeftSection />
                <MainFormContainer>
                    <FormBody mode={mode} />
                    <FormFooter />
                </MainFormContainer>
            </NewFormContainer>
        </ManageFormContextProvider>
    );
};

export default NewForm;
