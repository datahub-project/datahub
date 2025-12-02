import { Empty, Form, Select, message } from 'antd';
import React, { useRef, useState } from 'react';

import domainAutocompleteOptions from '@app/domainV2/DomainAutocompleteOptions';
import DomainNavigator from '@app/domainV2/nestedDomains/domainNavigator/DomainNavigator';
import { useEntityContext } from '@app/entity/shared/EntityContext';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { handleBatchError } from '@app/entityV2/shared/utils';
import ClickOutside from '@app/shared/ClickOutside';
import { BrowserWrapper } from '@app/shared/tags/AddTagsTermsModal';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Modal } from '@src/alchemy-components';
import analytics, { EntityActionType, EventType } from '@src/app/analytics';
import { getModalDomContainer } from '@src/utils/focus';

import { useBatchSetDomainMutation } from '@graphql/mutations.generated';
import { useGetAutoCompleteResultsLazyQuery } from '@graphql/search.generated';
import { DataHubPageModuleType, Domain, Entity, EntityType } from '@types';

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

export const SetDomainModal = ({ urns, onCloseModal, refetch, defaultValue, onOkOverride, titleOverride }: Props) => {
    const { reloadByKeyType } = useReloadableContext();
    const entityRegistry = useEntityRegistry();
    const { entityType } = useEntityContext();
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
    const [domainSearch, { data: domainSearchData, loading: searchLoading }] = useGetAutoCompleteResultsLazyQuery();
    const domainSearchResults: Entity[] = domainSearchData?.autoComplete?.entities || [];

    const [batchSetDomainMutation] = useBatchSetDomainMutation();
    const inputEl = useRef(null);
    const isShowingDomainNavigator = !inputValue && isFocusedOnInput;

    const onModalClose = () => {
        setInputValue('');
        setSelectedDomain(undefined);
        onCloseModal();
    };

    const handleSearch = (text: string) => {
        if (text) {
            domainSearch({
                variables: {
                    input: {
                        type: EntityType.Domain,
                        query: text,
                        limit: 5,
                    },
                },
            });
        }
    };

    const domainResult = !inputValue.length ? [] : domainSearchResults;

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

    const sendAnalytics = () => {
        const isBatchAction = urns.length > 1;

        if (isBatchAction) {
            analytics.event({
                type: EventType.BatchEntityActionEvent,
                actionType: EntityActionType.SetDomain,
                entityUrns: urns,
            });
        } else {
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.SetDomain,
                entityType,
                entityUrn: urns[0],
            });
        }
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
                    message.success({ content: 'Updated Domain!', duration: 2 });
                    refetch?.();
                    sendAnalytics();
                    onModalClose();
                    setSelectedDomain(undefined);
                    // Reload modules
                    // Assets - as assets module in domain summary tab could be updated
                    reloadByKeyType(
                        [getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets)],
                        3000,
                    );
                    // DataProduct - as data products module in domain summary tab could be updated
                    if (entityType === EntityType.DataProduct) {
                        reloadByKeyType(
                            [
                                getReloadableKeyType(
                                    ReloadableKeyTypeNamespace.MODULE,
                                    DataHubPageModuleType.DataProducts,
                                ),
                            ],
                            3000,
                        );
                    }
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: `Failed to add assets to Domain: \n ${e.message || ''}`,
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

    function handleClickOutside() {
        // delay closing the domain navigator so we don't get a UI "flash" between showing search results and navigator
        setTimeout(() => setIsFocusedOnInput(false), 0);
    }

    return (
        <Modal
            title={titleOverride || 'Set Domain'}
            open
            onCancel={onModalClose}
            buttons={[
                {
                    text: 'Cancel',
                    variant: 'text',
                    onClick: onModalClose,
                    buttonDataTestId: 'cancel-button',
                },
                {
                    text: 'Save',
                    variant: 'filled',
                    disabled: selectedDomain === undefined,
                    onClick: onOk,
                    id: 'setDomainButton',
                    buttonDataTestId: 'submit-button',
                },
            ]}
            getContainer={getModalDomContainer}
            data-testid="set-domain-modal"
        >
            <Form component={false}>
                <Form.Item>
                    <ClickOutside onClickOutside={handleClickOutside}>
                        <Select
                            autoFocus
                            showSearch
                            filterOption={false}
                            defaultActiveFirstOption={false}
                            placeholder="Search for Domains..."
                            onSelect={(domainUrn: any) => onSelectDomain(domainUrn)}
                            onDeselect={onDeselectDomain}
                            onSearch={(value: string) => {
                                handleSearch(value.trim());
                                setInputValue(value.trim());
                            }}
                            ref={inputEl}
                            value={selectValue}
                            onBlur={handleBlur}
                            onFocus={() => setIsFocusedOnInput(true)}
                            dropdownStyle={isShowingDomainNavigator ? { display: 'none' } : {}}
                            notFoundContent={
                                <Empty
                                    description="No Domains Found"
                                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                                    style={{ color: ANTD_GRAY[7] }}
                                />
                            }
                            options={domainAutocompleteOptions(domainResult, searchLoading, entityRegistry)}
                            data-testid="set-domain-select"
                        />
                        <BrowserWrapper isHidden={!isShowingDomainNavigator}>
                            <DomainNavigator selectDomainOverride={selectDomainFromBrowser} />
                        </BrowserWrapper>
                    </ClickOutside>
                </Form.Item>
            </Form>
        </Modal>
    );
};
