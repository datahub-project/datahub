import React from 'react';
import { useTranslation } from 'react-i18next';

import { ContainerSelectModal } from '@app/entity/shared/containers/profile/sidebar/Container/ContainerSelectModal';
import { SetDomainModal } from '@app/entity/shared/containers/profile/sidebar/Domain/SetDomainModal';
import { EditOwnersModal } from '@app/entity/shared/containers/profile/sidebar/Ownership/EditOwnersModal';
import { SelectPlatformModal } from '@app/entity/shared/containers/profile/sidebar/Platform/SelectPlatformModal';
import { ChooseEntityTypeModal } from '@app/search/ChooseEntityTypeModal';
import { EditTextModal } from '@app/search/EditTextModal';
import {
    CONTAINER_FILTER_NAME,
    DESCRIPTION_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    ENTITY_FILTER_NAME,
    FIELD_DESCRIPTIONS_FILTER_NAME,
    FIELD_GLOSSARY_TERMS_FILTER_NAME,
    FIELD_PATHS_FILTER_NAME,
    FIELD_TAGS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    REMOVED_FILTER_NAME,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
} from '@app/search/utils/constants';
import AddTagsModal from '@app/shared/tags/AddTagsModal';
import AddTermsModal from '@app/shared/tags/AddTermsModal';

import { FacetMetadata } from '@types';

type Props = {
    facet?: FacetMetadata | null;
    filterField: string;
    onSelect: (values: string[]) => void;
    onCloseModal: () => void;
    initialValues?: string[];
};

export const AdvancedFilterSelectValueModal = ({
    filterField,
    onSelect,
    onCloseModal,
    initialValues,
    facet,
}: Props) => {
    const { t } = useTranslation('search');

    if (filterField === OWNERS_FILTER_NAME) {
        return (
            <EditOwnersModal
                title={t('advancedFilter.selectOwners')}
                urns={[]}
                defaultValues={initialValues?.map((urn) => ({
                    urn,
                    entity: facet?.aggregations?.find((aggregation) => aggregation.value === urn)?.entity,
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
    if (filterField === DOMAINS_FILTER_NAME) {
        return (
            <SetDomainModal
                titleOverride={t('advancedFilter.selectDomain')}
                urns={[]}
                defaultValue={
                    initialValues?.map((urn) => ({
                        urn,
                        entity: facet?.aggregations?.find((aggregation) => aggregation.value === urn)?.entity,
                    }))?.[0]
                }
                onCloseModal={onCloseModal}
                onOkOverride={(domainUrn) => {
                    onSelect([domainUrn]);
                    onCloseModal();
                }}
            />
        );
    }

    if (filterField === CONTAINER_FILTER_NAME) {
        return (
            <ContainerSelectModal
                titleOverride={t('advancedFilter.selectContainer')}
                defaultValues={initialValues?.map((urn) => ({
                    urn,
                    entity: facet?.aggregations?.find((aggregation) => aggregation.value === urn)?.entity,
                }))}
                onCloseModal={onCloseModal}
                onOkOverride={(containerUrns) => {
                    onSelect(containerUrns);
                    onCloseModal();
                }}
            />
        );
    }

    if (filterField === PLATFORM_FILTER_NAME) {
        return (
            <SelectPlatformModal
                defaultValues={initialValues?.map((urn) => ({
                    urn,
                    entity: facet?.aggregations?.find((aggregation) => aggregation.value === urn)?.entity,
                }))}
                titleOverride={t('advancedFilter.selectPlatform')}
                onCloseModal={onCloseModal}
                onOk={(platformUrns) => {
                    onSelect(platformUrns);
                    onCloseModal();
                }}
            />
        );
    }

    if (filterField === FIELD_PATHS_FILTER_NAME) {
        return (
            <EditTextModal
                title={t('advancedFilter.filterByColumn')}
                defaultValue={initialValues?.[0]}
                onCloseModal={onCloseModal}
                onOk={(newValue) => {
                    onSelect([newValue]);
                    onCloseModal();
                }}
            />
        );
    }

    if (filterField === DESCRIPTION_FILTER_NAME || filterField === FIELD_DESCRIPTIONS_FILTER_NAME) {
        return (
            <EditTextModal
                title={t('advancedFilter.filterByDescription')}
                defaultValue={initialValues?.[0]}
                onCloseModal={onCloseModal}
                onOk={(newValue) => {
                    onSelect([newValue]);
                    onCloseModal();
                }}
            />
        );
    }

    if (filterField === ORIGIN_FILTER_NAME) {
        return (
            <EditTextModal
                title={t('advancedFilter.filterByEnvironment')}
                defaultValue={initialValues?.[0]}
                onCloseModal={onCloseModal}
                onOk={(newValue) => {
                    onSelect([newValue]);
                    onCloseModal();
                }}
            />
        );
    }

    if (filterField === TYPE_NAMES_FILTER_NAME) {
        return (
            <EditTextModal
                title={t('advancedFilter.filterBySubType')}
                defaultValue={initialValues?.[0]}
                onCloseModal={onCloseModal}
                onOk={(newValue) => {
                    onSelect([newValue]);
                    onCloseModal();
                }}
            />
        );
    }

    if (filterField === ENTITY_FILTER_NAME) {
        return (
            <ChooseEntityTypeModal
                title={t('advancedFilter.filterByEntityType')}
                defaultValues={initialValues}
                onCloseModal={onCloseModal}
                onOk={(newValues) => {
                    onSelect(newValues);
                    onCloseModal();
                }}
            />
        );
    }

    if (filterField === TAGS_FILTER_NAME || filterField === FIELD_TAGS_FILTER_NAME) {
        return (
            <AddTagsModal
                resources={[]}
                open
                onCloseModal={onCloseModal}
                onOkOverride={(urns) => {
                    onSelect(urns);
                    onCloseModal();
                }}
                defaultValues={initialValues?.map((urn) => ({
                    urn,
                    entity: facet?.aggregations?.find((aggregation) => aggregation.value === urn)?.entity,
                }))}
            />
        );
    }

    if (filterField === REMOVED_FILTER_NAME) {
        onSelect(['true']);
        onCloseModal();
    }

    if (filterField === GLOSSARY_TERMS_FILTER_NAME || filterField === FIELD_GLOSSARY_TERMS_FILTER_NAME) {
        return (
            <AddTermsModal
                resources={[]}
                open
                onCloseModal={onCloseModal}
                onOkOverride={(urns) => {
                    onSelect(urns);
                    onCloseModal();
                }}
                defaultValues={initialValues?.map((urn) => ({
                    urn,
                    entity: facet?.aggregations?.find((aggregation) => aggregation.value === urn)?.entity,
                }))}
            />
        );
    }
    return null;
};
