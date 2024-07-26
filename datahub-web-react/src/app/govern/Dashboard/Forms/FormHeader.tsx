import React from 'react';
import { Text } from '@components';
import { FormColorIcon, FormHeaderContainer, FormHeaderText } from './styledComponents';

const FormHeader = () => {
    return (
        <FormHeaderContainer>
            <FormColorIcon />
            <FormHeaderText>
                <Text color="white" size="lg" weight="black">
                    Form name
                </Text>
                <Text color="white" size="md" weight="light">
                    Description
                </Text>
            </FormHeaderText>
        </FormHeaderContainer>
    );
};

export default FormHeader;
