import { Icon, Pill, Text } from '@components';
import React, { useContext } from 'react';
import { Link } from 'react-router-dom';

import ManageFormContext from '@app/govern/Dashboard/Forms/ManageFormContext';
import { getStatusDetails } from '@app/govern/Dashboard/Forms/formUtils';
import { BreadcrumbContainer, Header } from '@app/govern/Dashboard/Forms/styledComponents';
import { PageRoutes } from '@src/conf/Global';
import { FormState } from '@src/types.generated';

const CreateFormHeader = () => {
    const { formMode, formValues, data } = useContext(ManageFormContext);

    const assignmentStatus = data?.form?.formAssignmentStatus;
    const formStatus = formValues.state || FormState.Draft;
    const { label, colorScheme } = getStatusDetails(formStatus, assignmentStatus);

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
                <Pill size="sm" label={label} color={colorScheme} />
            </Header>
            <Text size="md" color="gray">
                {`${formMode === 'create' ? 'Create a new' : 'Edit a'} compliance requirements form`}
            </Text>
        </>
    );
};

export default CreateFormHeader;
