import React from 'react';
import { LeftSection } from './styledComponents';
import LeftSectionHeader from './LeftSectionHeader';
import FormSteps from './FormSteps';

const FormLeftSection = () => {
    return (
        <LeftSection>
            <LeftSectionHeader />
            <FormSteps />
        </LeftSection>
    );
};

export default FormLeftSection;
