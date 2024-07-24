import React from 'react';
import { MainFormContainer, NewFormContainer } from './styledComponents';
import FormLeftSection from './FormLeftSection';
import FormBody from './FormBody';
import FormFooter from './FormFooter';

const NewForm = () => {
    return (
        <NewFormContainer>
            <FormLeftSection />
            <MainFormContainer>
                <FormBody />
                <FormFooter />
            </MainFormContainer>
        </NewFormContainer>
    );
};

export default NewForm;
