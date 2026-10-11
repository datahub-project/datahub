import { Input } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

interface ApplicationDetailsProps {
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
    const { t } = useTranslation('misc');
    const { t: tl } = useTranslation('common.labels');
    return (
        <SectionContainer>
            <FormSection>
                <Input
                    label={tl('name')}
                    inputTestId="application-name-input"
                    value={applicationName}
                    setValue={setApplicationName}
                    placeholder={t('applications.namePlaceholder')}
                    required
                />
            </FormSection>

            <FormSection>
                <Input
                    inputTestId="application-description-input"
                    label={tl('description')}
                    value={applicationDescription}
                    setValue={setApplicationDescription}
                    placeholder={t('applications.descriptionPlaceholder')}
                    type="textarea"
                />
            </FormSection>
        </SectionContainer>
    );
};

export default ApplicationDetailsSection;
