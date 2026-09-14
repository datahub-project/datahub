import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import ResourceItemPill from '@app/entityV2/shared/tabs/Documentation/components/ResourceItemPill';
import {
    ALL_RESOURCE_TYPES,
    filterResourceGroups,
    getResourceTypeOptions,
    groupResources,
} from '@app/entityV2/shared/tabs/Documentation/components/ResourcesModal.utils';
import { RelatedItem } from '@app/entityV2/shared/tabs/Documentation/components/relatedSectionUtils';
import { Modal, Pill, SearchBar, SimpleSelect, Text } from '@src/alchemy-components';

import { InstitutionalMemoryMetadata } from '@types';

const ModalContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 20px;
    min-height: 360px;
`;

const Toolbar = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
`;

const SearchWrapper = styled.div`
    flex: 1 1 200px;
`;

const Sections = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
    max-height: 480px;
    overflow-y: auto;
`;

const Section = styled.section`
    display: flex;
    flex-direction: column;
    gap: 10px;
`;

const SectionHeading = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const ResourceList = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
`;

const EmptyState = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 240px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

type Props = {
    items: RelatedItem[];
    onClose: () => void;
    onDocumentClick: (documentUrn: string) => void;
    onDocumentRemove: (documentUrn: string) => void;
    canRemoveDocuments: boolean;
    onLinkEdit: (link: InstitutionalMemoryMetadata) => void;
    onLinkDelete: (link: InstitutionalMemoryMetadata) => void;
};

/**
 * Full list of an entity's Resources, grouped into Documents and Links, with search
 * and type filtering.
 */
export default function ResourcesModal({
    items,
    onClose,
    onDocumentClick,
    onDocumentRemove,
    canRemoveDocuments,
    onLinkEdit,
    onLinkDelete,
}: Props) {
    const { t } = useTranslation('entity.profile.summary');
    const { t: tc } = useTranslation('common.actions');
    const [query, setQuery] = useState('');
    const [selectedType, setSelectedType] = useState(ALL_RESOURCE_TYPES);

    const groupLabels = useMemo(() => ({ documents: t('links.documents'), links: t('links.links') }), [t]);

    const groups = useMemo(() => groupResources(items, groupLabels), [items, groupLabels]);

    const typeOptions = useMemo(
        () => [
            { value: ALL_RESOURCE_TYPES, label: t('links.allTypes') },
            ...getResourceTypeOptions(items, groupLabels),
        ],
        [groupLabels, items, t],
    );

    const visibleGroups = useMemo(
        () => filterResourceGroups(groups, selectedType, query),
        [groups, query, selectedType],
    );

    return (
        <Modal
            title={t('links.resourcesTitle')}
            titlePill={<Pill label={String(items.length)} size="sm" color="gray" variant="filled" />}
            width={760}
            onCancel={onClose}
            buttons={[{ text: tc('close'), variant: 'text', color: 'gray', onClick: onClose }]}
            dataTestId="resources-modal"
        >
            <ModalContent>
                <Toolbar>
                    <SearchWrapper>
                        <SearchBar
                            value={query}
                            onChange={setQuery}
                            placeholder={t('links.searchPlaceholder')}
                            data-testid="resources-search-input"
                        />
                    </SearchWrapper>
                    <SimpleSelect
                        size="sm"
                        width="fit-content"
                        options={typeOptions}
                        values={[selectedType]}
                        onUpdate={(values) => setSelectedType(values?.[0] || ALL_RESOURCE_TYPES)}
                        showClear={false}
                        placeholder={t('links.filterPlaceholder')}
                        dataTestId="resources-type-filter"
                    />
                </Toolbar>

                {visibleGroups.length > 0 ? (
                    <Sections>
                        {visibleGroups.map((group) => (
                            <Section key={group.key}>
                                <SectionHeading>
                                    <Text weight="bold" size="sm">
                                        {group.label}
                                    </Text>
                                    <Pill label={String(group.items.length)} size="sm" color="gray" variant="filled" />
                                </SectionHeading>
                                <ResourceList>
                                    {group.items.map((item) => (
                                        <ResourceItemPill
                                            key={
                                                item.type === 'link'
                                                    ? `link-${item.data.url}`
                                                    : `document-${item.data.urn}`
                                            }
                                            item={item}
                                            canRemoveDocuments={canRemoveDocuments}
                                            onDocumentClick={onDocumentClick}
                                            onDocumentRemove={onDocumentRemove}
                                            onLinkEdit={onLinkEdit}
                                            onLinkDelete={onLinkDelete}
                                        />
                                    ))}
                                </ResourceList>
                            </Section>
                        ))}
                    </Sections>
                ) : (
                    <EmptyState>
                        <Text>{t('links.noResults')}</Text>
                    </EmptyState>
                )}
            </ModalContent>
        </Modal>
    );
}
