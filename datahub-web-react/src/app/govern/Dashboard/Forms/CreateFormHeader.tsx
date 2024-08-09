import { Icon, Text } from '@src/alchemy-components';
import React, { useContext } from 'react';
import { useHistory } from 'react-router-dom';
import ManageFormContext from './ManageFormContext';
import { BackText, BreadcrumbContainer } from './styledComponents';

const CreateFormHeader = () => {
    const { formMode } = useContext(ManageFormContext);

    const history = useHistory();

    const handleGoBack = () => {
        (history as any)?.goBack();
    };

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
            <Text size="2xl" weight="bold">
                Forms
            </Text>
            <Text size="md" color="gray">
                {`${formMode === 'create' ? 'Create' : 'Edit'} forms to be utilized`}
            </Text>
        </>
    );
};

export default CreateFormHeader;
