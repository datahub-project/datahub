import React from 'react';
import { FormView, useEntityFormContext } from '@src/app/entity/shared/entityForm/EntityFormContext';
import FormByEntity from '@src/app/entity/shared/entityForm/FormByEntity';

interface Props {
    formUrn: string;
}

export default function EntityForm({ formUrn }: Props) {
    const { formView } = useEntityFormContext();

    if (formView === FormView.BY_ENTITY) return <FormByEntity formUrn={formUrn} />;

    return null;
}
