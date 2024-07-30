import React from 'react';
import { LeftSection } from './styledComponents';
import LeftSectionHeader from './LeftSectionHeader';
import FormStepsView from './FormStepsView';

const FormLeftSection = () => {
    return (
        <LeftSection>
            <LeftSectionHeader />
            <FormStepsView />
        </LeftSection>
    );
};

export default FormLeftSection;
