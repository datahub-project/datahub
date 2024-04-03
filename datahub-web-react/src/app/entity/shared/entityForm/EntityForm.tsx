import React from 'react';
import FormByEntity from './FormByEntity';
import { FormView, useEntityFormContext } from './EntityFormContext';

interface Props {
    formUrn: string;
}

export default function EntityForm({ formUrn }: Props) {
    const { formView } = useEntityFormContext();

    if (formView === FormView.BY_ENTITY) return <FormByEntity formUrn={formUrn} />;

    return null;
}
