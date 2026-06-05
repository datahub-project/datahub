import { Loader } from '@components';
import { Button, Empty, Form, Modal, Select, message } from 'antd';
import React, { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components/macro';

import DomainNavigator from '@app/domain/nestedDomains/domainNavigator/DomainNavigator';
import { getParentDomains } from '@app/domain/utils';
import { tagRender } from '@app/entity/shared/containers/profile/sidebar/tagRenderer';
import { handleBatchError } from '@app/entity/shared/utils';
import ParentEntities from '@app/search/filters/ParentEntities';
import ClickOutside from '@app/shared/ClickOutside';
import { DomainLabel } from '@app/shared/DomainLabel';
import { BrowserWrapper } from '@app/shared/tags/BrowserWrapper';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { getModalDomContainer } from '@utils/focus';

import { useBatchSetDomainMutation } from '@graphql/mutations.generated';
import { useGetSearchResultsLazyQuery } from '@graphql/search.generated';
import { Domain, Entity, EntityType } from '@types';

type Props = {
    urns: string[];
    onCloseModal: () => void;
    refetch?: () => Promise<any>;
    defaultValue?: { urn: string; entity?: Entity | null };
    onOkOverride?: (result: string) => void;
    titleOverride?: string;
};

type SelectedDomain = {
    displayName: string;
    type: EntityType;
    urn: string;
};

const SearchResultContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
`;

// Programmatic Select.Option value for the loading row — not display copy.
const LOADING_OPTION_VALUE = 'loading';

export const SetDomainModal = ({ urns, onCloseModal, refetch, defaultValue, onOkOverride, titleOverride }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();
    const entityRegistry = useEntityRegistry();
    const [isFocusedOnInput, setIsFocusedOnInput] = useState(false);
    const [inputValue, setInputValue] = useState('');
    const [selectedDomain, setSelectedDomain] = useState<SelectedDomain | undefined>(
        defaultValue
            ? {
                  displayName: entityRegistry.getDisplayName(EntityType.Domain, defaultValue?.entity),
                  type: EntityType.Domain,
                  urn: defaultValue?.urn,
              }
            : undefined,
    );
    const [domainSearch, { data: domainSearchData, loading }] = useGetSearchResultsLazyQuery();
    const domainSearchResults =
        domainSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const [batchSetDomainMutation] = useBatchSetDomainMutation();
    const inputEl = useRef(null);
    const isShowingDomainNavigator = !inputValue && isFocusedOnInput;

    const onModalClose = () => {
        setInputValue('');
        setSelectedDomain(undefined);
        onCloseModal();
    };

    const handleSearch = (text: string) => {
        domainSearch({
            variables: {
                input: {
                    type: EntityType.Domain,
                    query: text,
                    start: 0,
                    count: 5,
                },
            },
        });
    };

    // Renders a search result in the select dropdown.
    const renderSearchResult = (entity: Entity) => {
        const displayName = entityRegistry.getDisplayName(entity.type, entity);
        return (
            <Select.Option value={entity.urn} key={entity.urn}>
                <SearchResultContainer>
                    <ParentEntities parentEntities={getParentDomains(entity, entityRegistry)} />
                    <DomainLabel name={displayName} />
                </SearchResultContainer>
            </Select.Option>
        );
    };

    const domainResult = !inputValue || inputValue.length === 0 ? [] : domainSearchResults;

    const domainSearchOptions = domainResult?.map((result) => {
        return renderSearchResult(result);
    });

    const onSelectDomain = (newUrn: string) => {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const filteredDomains = domainResult?.filter((entity) => entity.urn === newUrn).map((entity) => entity) || [];
        if (filteredDomains.length) {
            const domain = filteredDomains[0];
            setSelectedDomain({
                displayName: entityRegistry.getDisplayName(EntityType.Domain, domain),
                type: EntityType.Domain,
                urn: newUrn,
            });
        }
    };

    function selectDomainFromBrowser(domain: Domain) {
        setIsFocusedOnInput(false);
        setSelectedDomain({
            displayName: entityRegistry.getDisplayName(EntityType.Domain, domain),
            type: EntityType.Domain,
            urn: domain.urn,
        });
    }

    const onDeselectDomain = () => {
        setInputValue('');
        setSelectedDomain(undefined);
    };

    const onOk = () => {
        if (!selectedDomain) {
            return;
        }

        if (onOkOverride) {
            onOkOverride(selectedDomain?.urn);
            return;
        }

        batchSetDomainMutation({
            variables: {
                input: {
                    resources: [...urns.map((urn) => ({ resourceUrn: urn }))],
                    domainUrn: selectedDomain.urn,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: t('sidebar.domain.updatedSuccess'), duration: 2 });
                    refetch?.();
                    onModalClose();
                    setSelectedDomain(undefined);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: t('sidebar.domain.addFailed', { message: e.message || '' }),
                        duration: 3,
                    }),
                );
            });
    };

    const selectValue = (selectedDomain && [selectedDomain?.displayName]) || undefined;

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#setDomainButton',
    });

    function handleBlur() {
        setInputValue('');
    }

    function handleCLickOutside() {
        // delay closing the domain navigator so we don't get a UI "flash" between showing search results and navigator
        setTimeout(() => setIsFocusedOnInput(false), 0);
    }

    return (
        <Modal
            title={titleOverride || t('sidebar.domain.setModalTitle')}
            open
            onCancel={onModalClose}
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
                        {tc('cancel')}
                    </Button>
                    <Button id="setDomainButton" disabled={selectedDomain === undefined} onClick={onOk}>
                        {tc('add')}
                    </Button>
                </>
            }
            getContainer={getModalDomContainer}
        >
            <Form component={false}>
                <Form.Item>
                    <ClickOutside onClickOutside={handleCLickOutside}>
                        <Select
                            autoFocus
                            defaultOpen
                            filterOption={false}
                            showSearch
                            mode="multiple"
                            defaultActiveFirstOption={false}
                            placeholder={t('sidebar.domain.searchPlaceholder')}
                            onSelect={(domainUrn: any) => onSelectDomain(domainUrn)}
                            onDeselect={onDeselectDomain}
                            onSearch={(value: string) => {
                                // eslint-disable-next-line react/prop-types
                                handleSearch(value.trim());
                                // eslint-disable-next-line react/prop-types
                                setInputValue(value.trim());
                            }}
                            ref={inputEl}
                            value={selectValue}
                            tagRender={tagRender}
                            onBlur={handleBlur}
                            onFocus={() => setIsFocusedOnInput(true)}
                            dropdownStyle={isShowingDomainNavigator ? { display: 'none' } : {}}
                            notFoundContent={
                                <Empty
                                    description={t('sidebar.domain.emptyText')}
                                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                                    style={{ color: theme.colors.textSecondary }}
                                />
                            }
                        >
                            {loading ? (
                                <Select.Option value={LOADING_OPTION_VALUE}>
                                    <Loader size="xs" padding={8} />
                                </Select.Option>
                            ) : (
                                domainSearchOptions
                            )}
                        </Select>
                        <BrowserWrapper isHidden={!isShowingDomainNavigator}>
                            <DomainNavigator selectDomainOverride={selectDomainFromBrowser} />
                        </BrowserWrapper>
                    </ClickOutside>
                </Form.Item>
            </Form>
        </Modal>
    );
};
