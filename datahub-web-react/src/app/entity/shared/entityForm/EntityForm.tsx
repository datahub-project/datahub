import React from 'react';

import { EntityContext, useEntityContext } from '../EntityContext';

import FormByEntity from './FormByEntity';
import { FormView, useEntityFormContext } from './EntityFormContext';
import FormByQuestion from './FormByQuestion';
import BulkVerify from './BulkVerify/BulkVerify';

interface Props {
    formUrn: string;
    closeModal: () => void;
}

export default function EntityForm({ formUrn, closeModal }: Props) {
    const {
        loading,
        form: { formView },
        entity: { refetch, selectedEntity, entityData: selectedEntityData },
    } = useEntityFormContext();
    const { entityType } = useEntityContext();

    return (
        <EntityContext.Provider
            value={{
                urn: selectedEntity?.urn || '',
                entityType: selectedEntity?.type || entityType,
                entityData: selectedEntityData || null,
                loading,
                baseEntity: selectedEntityData,
                dataNotCombinedWithSiblings: selectedEntityData,
                routeToTab: () => {},
                refetch,
            }}
        >
            {formView === FormView.BY_ENTITY && <FormByEntity formUrn={formUrn} />}
            {formView === FormView.BY_QUESTION && <FormByQuestion closeModal={closeModal} />}
            {formView === FormView.BULK_VERIFY && <BulkVerify closeFormModal={closeModal} />}
        </EntityContext.Provider>
    );
}
