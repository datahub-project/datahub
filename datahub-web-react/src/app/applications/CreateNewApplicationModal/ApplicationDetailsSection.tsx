/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Input } from '@components';
import React from 'react';
import styled from 'styled-components';

export interface ApplicationDetailsProps {
    applicationName: string;
    setApplicationName: React.Dispatch<React.SetStateAction<string>>;
    applicationDescription: string;
    setApplicationDescription: React.Dispatch<React.SetStateAction<string>>;
}

const SectionContainer = styled.div`
    margin-bottom: 24px;
`;

const FormSection = styled.div`
    margin-bottom: 16px;
`;

/**
 * Component for application name and description
 */
const ApplicationDetailsSection: React.FC<ApplicationDetailsProps> = ({
    applicationName,
    setApplicationName,
    applicationDescription,
    setApplicationDescription,
}) => {
    return (
        <SectionContainer>
            <FormSection>
                <Input
                    label="Name"
                    inputTestId="application-name-input"
                    value={applicationName}
                    setValue={setApplicationName}
                    placeholder="Enter application name"
                    required
                />
            </FormSection>

            <FormSection>
                <Input
                    inputTestId="application-description-input"
                    label="Description"
                    value={applicationDescription}
                    setValue={setApplicationDescription}
                    placeholder="Add a description for your new application"
                    type="textarea"
                />
            </FormSection>
        </SectionContainer>
    );
};

export default ApplicationDetailsSection;
