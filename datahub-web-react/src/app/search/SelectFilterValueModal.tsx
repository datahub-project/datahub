import React from 'react';
import { FacetMetadata } from '../../types.generated';
import { EditOwnersModal } from '../entity/shared/containers/profile/sidebar/Ownership/EditOwnersModal';
import { EditTextModal } from './EditTextModal';

type Props = {
    facet: FacetMetadata;
    filterField: string;
    onSelect: (values: string[]) => void;
    onCloseModal: () => void;
    initialValues?: string[];
};

export const SelectFilterValueModal = ({ filterField, onSelect, onCloseModal, initialValues, facet }: Props) => {
    if (filterField === 'owners') {
        return (
            <EditOwnersModal
                title="Select Owners"
                urns={[]}
                defaultValues={initialValues?.map((urn) => ({
                    urn,
                    entity: facet.aggregations.find((aggregation) => aggregation.value === urn)?.entity,
                }))}
                onCloseModal={onCloseModal}
                hideOwnerType
                onOkOverride={(owners) => {
                    onSelect(owners.map((owner) => owner.value.ownerUrn));
                    onCloseModal();
                }}
            />
        );
    }
    if (filterField === 'fieldPaths') {
        return (
            <EditTextModal
                title="Filter by Column"
                defaultValue={initialValues?.[0]}
                onCloseModal={onCloseModal}
                onOkOverride={(newValue) => {
                    onSelect([newValue]);
                    onCloseModal();
                }}
            />
        );
    }
    return null;
};
