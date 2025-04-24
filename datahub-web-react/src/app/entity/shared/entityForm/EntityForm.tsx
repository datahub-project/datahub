import React from 'react';

import { EntityContext, useEntityContext } from '@app/entity/shared/EntityContext';
import BulkVerify from '@app/entity/shared/entityForm/BulkVerify/BulkVerify';
import { FormView, useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import FormByEntity from '@app/entity/shared/entityForm/FormByEntity';
import FormByQuestion from '@app/entity/shared/entityForm/FormByQuestion';

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
