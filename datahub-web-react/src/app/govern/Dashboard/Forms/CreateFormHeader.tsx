import { Icon, Pill, Text } from '@components';
import { ColorOptions } from '@components/theme/config';
import { FormState } from '@src/types.generated';
import React, { useContext } from 'react';
import { useHistory } from 'react-router-dom';
import ManageFormContext from './ManageFormContext';
import { BackText, BreadcrumbContainer, Header } from './styledComponents';

const CreateFormHeader = () => {
    const { formMode, formValues } = useContext(ManageFormContext);

    const history = useHistory();

    const handleGoBack = () => {
        (history as any)?.goBack();
    };

    const formStatus = formValues.state || FormState.Draft;
    let colorScheme: ColorOptions = 'gray';
    if (formStatus === FormState.Published) colorScheme = 'violet';
    else if (formStatus === FormState.Unpublished) colorScheme = 'blue';

    return (
        <>
            <BreadcrumbContainer>
                <BackText onClick={() => handleGoBack()}>
                    <Text size="sm" color="gray">
                        Forms
                    </Text>
                </BackText>
                <Icon icon="ChevronRight" size="xl" color="gray" />
                <Text size="sm" color="gray">
                    {formMode === 'create' ? 'Create' : 'Edit'}
                </Text>
            </BreadcrumbContainer>
            <Header>
                <Text size="2xl" weight="bold">
                    Forms
                </Text>
                <Pill
                    size="sm"
                    label={formStatus.charAt(0) + formStatus.slice(1).toLowerCase()}
                    colorScheme={colorScheme}
                />
            </Header>
            <Text size="md" color="gray">
                {`${formMode === 'create' ? 'Create' : 'Edit'} forms to be utilized`}
            </Text>
        </>
    );
};

export default CreateFormHeader;
