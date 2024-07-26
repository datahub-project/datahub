import React from 'react';
import { MainFormContainer, NewFormContainer } from './styledComponents';
import FormLeftSection from './FormLeftSection';
import FormBody from './FormBody';
import FormFooter from './FormFooter';
import { ManageFormContextProvider } from './ManageFormContextProvider';

const NewForm = () => {
    return (
        <ManageFormContextProvider>
            <NewFormContainer>
                <FormLeftSection />
                <MainFormContainer>
                    <FormBody />
                    <FormFooter />
                </MainFormContainer>
            </NewFormContainer>
        </ManageFormContextProvider>
    );
};

export default NewForm;
