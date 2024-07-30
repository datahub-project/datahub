import React, { useContext } from 'react';
import { FormColorIcon, FormDescription, FormHeaderContainer, FormHeaderText, FormName } from './styledComponents';
import ManageFormContext from './ManageFormContext';

const FormHeader = () => {
    const { formValues } = useContext(ManageFormContext);

    return (
        <FormHeaderContainer>
            <FormColorIcon />
            <FormHeaderText>
                <FormName $width="65%">{formValues.formName || 'Form Name'}</FormName>
                <FormDescription $width="80%">{formValues.formDescription || 'Description'}</FormDescription>
            </FormHeaderText>
        </FormHeaderContainer>
    );
};

export default FormHeader;
