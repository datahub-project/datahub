import React from 'react';
import { FormHeaderContainer, HeaderText, SubText } from './styledComponents';

const FormHeader = () => {
    return (
        <FormHeaderContainer>
            <HeaderText>Form name</HeaderText>
            <SubText>Description</SubText>
        </FormHeaderContainer>
    );
};

export default FormHeader;
