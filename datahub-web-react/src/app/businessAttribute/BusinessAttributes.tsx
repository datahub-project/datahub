import { Button, EmptyState, PageTitle, Pagination, SearchBar, Table, toast } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';

import BusinessAttributeItemMenu from '@app/businessAttribute/BusinessAttributeItemMenu';
import CreateBusinessAttributeModal from '@app/businessAttribute/CreateBusinessAttributeModal';
import useDescriptionRenderer from '@app/businessAttribute/utils/useDescriptionRenderer';
import { useUserContext } from '@app/context/useUserContext';
import {
    ButtonContainer,
    HeaderContainer,
    HeaderContent,
    PageContainer,
} from '@app/govern/structuredProperties/styledComponents';
import TagTermGroup from '@app/sharedV2/tags/TagTermGroup';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';

import { useListBusinessAttributesQuery } from '@graphql/businessAttribute.generated';
import { BusinessAttribute, EntityType } from '@types';

const DEFAULT_PAGE_SIZE = 10;

export const BusinessAttributes = () => {
    const { t } = useTranslation('misc');
    const { t: tl } = useTranslation('common.labels');
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [isCreatingBusinessAttribute, setIsCreatingBusinessAttribute] = useState(false);
    const entityRegistry = useEntityRegistry();

    const authenticatedUser = useUserContext();
    const canCreateBusinessAttributes = authenticatedUser?.platformPrivileges?.createBusinessAttributes;

    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);
    const [searchQuery, setSearchQuery] = useState('');
    const [debouncedQuery, setDebouncedQuery] = useState('');

    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedQuery(searchQuery);
            setPage(1);
        }, 300);
        return () => clearTimeout(timer);
    }, [searchQuery]);

    const {
        loading: businessAttributeLoading,
        error: businessAttributeError,
        data: businessAttributeData,
        refetch: businessAttributeRefetch,
    } = useListBusinessAttributesQuery({
        variables: {
            start: (page - 1) * pageSize,
            count: pageSize,
            query: debouncedQuery || undefined,
        },
    });
    const descriptionRender = useDescriptionRenderer(businessAttributeRefetch);

    useEffect(() => {
        if (businessAttributeError) {
            toast.error(t('businessAttribute.loadError'));
        }
    }, [businessAttributeError, t]);

    const totalBusinessAttributes = businessAttributeData?.listBusinessAttributes?.total || 0;
    const businessAttributes = useMemo(
        () => (businessAttributeData?.listBusinessAttributes?.businessAttributes || []) as BusinessAttribute[],
        [businessAttributeData],
    );

    const handleDelete = () => {
        setTimeout(() => {
            businessAttributeRefetch?.();
        }, 2000);
    };

    const refreshTagsAndTerms = () => businessAttributeRefetch();

    const tableColumns = [
        {
            width: '20%',
            title: tl('name'),
            key: 'name',
            render: (record: BusinessAttribute) => (
                <Link to={entityRegistry.getEntityUrl(EntityType.BusinessAttribute, record.urn)}>
                    {record.properties?.name}
                </Link>
            ),
        },
        {
            width: '20%',
            title: tl('description'),
            key: 'description',
            render: (record: BusinessAttribute) => descriptionRender(record.properties?.description || '', record),
        },
        {
            width: '20%',
            title: tl('tags'),
            key: 'tags',
            render: (record: BusinessAttribute) => (
                <div data-testid={`schema-field-${record.properties?.name}-tags`}>
                    <TagTermGroup
                        editableTags={record.properties?.tags}
                        canRemove
                        canAddTag
                        entityUrn={record.urn}
                        entityType={EntityType.BusinessAttribute}
                        highlightText={debouncedQuery}
                        refetch={refreshTagsAndTerms}
                        fontSize={12}
                    />
                </div>
            ),
        },
        {
            width: '20%',
            title: t('businessAttribute.columnGlossaryTerms'),
            key: 'glossaryTerms',
            render: (record: BusinessAttribute) => (
                <div data-testid={`schema-field-${record.properties?.name}-terms`}>
                    <TagTermGroup
                        editableGlossaryTerms={record.properties?.glossaryTerms}
                        canRemove
                        canAddTerm
                        entityUrn={record.urn}
                        entityType={EntityType.BusinessAttribute}
                        highlightText={debouncedQuery}
                        refetch={refreshTagsAndTerms}
                        fontSize={12}
                    />
                </div>
            ),
        },
        {
            width: '13%',
            title: t('businessAttribute.dataType'),
            key: 'businessAttributeDataType',
            // listBusinessAttributes aliases `type` to `businessAttributeDataType`, which BusinessAttributeInfo lacks.
            render: (record: BusinessAttribute) =>
                (record.properties as { businessAttributeDataType?: string } | undefined)?.businessAttributeDataType ||
                '',
        },
        {
            width: '5%',
            title: '',
            key: 'menu',
            alignment: 'right' as AlignmentOptions,
            render: (record: BusinessAttribute) => (
                <BusinessAttributeItemMenu title={record.properties?.name} urn={record.urn} onDelete={handleDelete} />
            ),
        },
    ];

    const showEmptyState = !businessAttributeLoading && businessAttributes.length === 0;

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <HeaderContainer>
                <HeaderContent>
                    <PageTitle
                        title={t('businessAttribute.pageTitle')}
                        subTitle={t('businessAttribute.pageSubtitle')}
                    />
                </HeaderContent>
                <ButtonContainer>
                    <Button
                        size="md"
                        color="primary"
                        icon={{ icon: Plus }}
                        onClick={() => setIsCreatingBusinessAttribute(true)}
                        data-testid="add-business-attribute-button"
                        disabled={!canCreateBusinessAttributes}
                    >
                        {t('businessAttribute.createButton')}
                    </Button>
                </ButtonContainer>
            </HeaderContainer>
            <SearchBar
                placeholder={t('businessAttribute.searchPlaceholder')}
                value={searchQuery}
                onChange={setSearchQuery}
                width="280px"
            />
            {showEmptyState ? (
                <EmptyState title={t('businessAttribute.empty')} />
            ) : (
                <>
                    <Table
                        columns={tableColumns}
                        data={businessAttributes}
                        rowKey="urn"
                        isLoading={businessAttributeLoading && !businessAttributeData}
                        isScrollable
                    />
                    <Pagination
                        currentPage={page}
                        itemsPerPage={pageSize}
                        total={totalBusinessAttributes}
                        loading={businessAttributeLoading}
                        onPageChange={(newPage, newPageSize) => {
                            if (newPageSize !== pageSize) {
                                setPage(1);
                                setPageSize(newPageSize);
                            } else {
                                setPage(newPage);
                            }
                        }}
                    />
                </>
            )}
            <CreateBusinessAttributeModal
                open={isCreatingBusinessAttribute}
                onClose={() => setIsCreatingBusinessAttribute(false)}
                onCreateBusinessAttribute={() => {
                    businessAttributeRefetch?.();
                }}
            />
        </PageContainer>
    );
};
