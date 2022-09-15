import React from 'react';
import { FacetMetadata, EntityType } from '../../types.generated';
import { EditOwnersModal } from '../entity/shared/containers/profile/sidebar/Ownership/EditOwnersModal';
import EditTagTermsModal from '../shared/tags/AddTagsTermsModal';
import { EditTextModal } from './EditTextModal';

type Props = {
    facet?: FacetMetadata | null;
    filterField: string;
    onSelect: (values: string[]) => void;
    onCloseModal: () => void;
    initialValues?: string[];
};

export const SelectFilterValueModal = ({ filterField, onSelect, onCloseModal, initialValues, facet }: Props) => {
    console.log(facet?.aggregations);

    if (filterField === 'owners') {
        return (
            <EditOwnersModal
                title="Select Owners"
                urns={[]}
                defaultValues={initialValues?.map((urn) => ({
                    urn,
                    entity: facet?.aggregations.find((aggregation) => aggregation.value === urn)?.entity,
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
    if (filterField === 'tags' || filterField === 'fieldTags') {
        return (
            <EditTagTermsModal
                resources={[]}
                type={EntityType.Tag}
                visible
                onCloseModal={onCloseModal}
                onOkOverride={(urns) => {
                    onSelect(urns);
                    onCloseModal();
                }}
                defaultValues={initialValues?.map((urn) => ({
                    urn,
                    entity: facet?.aggregations.find((aggregation) => aggregation.value === urn)?.entity,
                }))}
            />
        );
    }

    if (filterField === 'glossaryTerms' || filterField === 'fieldGlossaryTerms') {
        return (
            <EditTagTermsModal
                resources={[]}
                type={EntityType.GlossaryTerm}
                visible
                onCloseModal={onCloseModal}
                onOkOverride={(urns) => {
                    onSelect(urns);
                    onCloseModal();
                }}
                defaultValues={initialValues?.map((urn) => ({
                    urn,
                    entity: facet?.aggregations.find((aggregation) => aggregation.value === urn)?.entity,
                }))}
            />
        );
    }
    return null;
};
