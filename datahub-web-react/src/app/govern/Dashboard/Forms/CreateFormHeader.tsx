import { Icon, Pill, Text } from '@components';
import { ColorOptions } from '@components/theme/config';
import { FormState } from '@src/types.generated';
import React, { useContext } from 'react';
import { Link } from 'react-router-dom';
import { PageRoutes } from '@src/conf/Global';
import ManageFormContext from './ManageFormContext';
import { BreadcrumbContainer, Header } from './styledComponents';

const CreateFormHeader = () => {
    const { formMode, formValues } = useContext(ManageFormContext);

    const formStatus = formValues.state || FormState.Draft;
    let colorScheme: ColorOptions = 'gray';
    if (formStatus === FormState.Published) colorScheme = 'violet';
    else if (formStatus === FormState.Unpublished) colorScheme = 'blue';

    return (
        <>
            <BreadcrumbContainer>
                <Link to={`${PageRoutes.GOVERN_DASHBOARD}?documentationTab=forms`}>
                    <Text size="lg" color="gray">
                        Compliance Forms
                    </Text>
                </Link>
                <Icon icon="ChevronRight" size="xl" color="gray" />
                <Text size="lg" color="gray">
                    {formMode === 'create' ? 'Create' : 'Edit'}
                </Text>
            </BreadcrumbContainer>
            <Header>
                <Text size="2xl" weight="bold" data-testid="form-title">
                    Compliance Forms
                </Text>
                <Pill
                    size="sm"
                    label={formStatus.charAt(0) + formStatus.slice(1).toLowerCase()}
                    colorScheme={colorScheme}
                />
            </Header>
            <Text size="md" color="gray">
                {`${formMode === 'create' ? 'Create a new' : 'Edit a'} compliance requirements form`}
            </Text>
        </>
    );
};

export default CreateFormHeader;
