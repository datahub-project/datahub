import React, { useRef, useState } from 'react';
import { Form, message, Modal, Select, Empty } from 'antd';
import { getModalDomContainer } from '@src/utils/focus';
<<<<<<< HEAD
import { useProposeDomainMutation } from '@src/graphql/domain.generated';
import { useEntityContext, useMutationUrn } from '@src/app/entity/shared/EntityContext';
import analytics, { EventType } from '@src/app/analytics';
import handleGraphQLError from '@src/app/shared/handleGraphQLError';
||||||| f14c42d2ef7
=======
import { Button } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
>>>>>>> master
import { useGetAutoCompleteResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { Domain, Entity, EntityType } from '../../../../../../../types.generated';
import { useBatchSetDomainMutation } from '../../../../../../../graphql/mutations.generated';
import domainAutocompleteOptions from '../../../../../../domainV2/DomainAutocompleteOptions';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { useEnterKeyListener } from '../../../../../../shared/useEnterKeyListener';
import { handleBatchError } from '../../../../utils';
import { BrowserWrapper } from '../../../../../../shared/tags/AddTagsTermsModal';
import DomainNavigator from '../../../../../../domain/nestedDomains/domainNavigator/DomainNavigator';
import ClickOutside from '../../../../../../shared/ClickOutside';
import { ANTD_GRAY } from '../../../../constants';

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
    const entityRegistry = useEntityRegistry();
    const { refetch: entityRefetch } = useEntityContext();
    const mutationUrn = useMutationUrn();
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
    const [proposeDomainMutation] = useProposeDomainMutation();
    const inputEl = useRef(null);
    const isShowingDomainNavigator = !inputValue && isFocusedOnInput;

    const onModalClose = () => {
        setInputValue('');
        setSelectedDomain(undefined);
        onCloseModal();
    };

    const handleSearch = (text: string) => {
        console.log('Not calling search');
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

    const proposeDomain = () => {
        message.loading('Proposing...');

        if (!selectedDomain) {
            return;
        }

        // TODO: Add description to the proposal
        proposeDomainMutation({
            variables: {
                input: {
                    resourceUrn: mutationUrn,
                    domainUrn: selectedDomain.urn,
                },
            },
        })
            .then(() => {
                analytics.event({
                    type: EventType.ProposeDomainMutation,
                    resourceUrn: mutationUrn,
                    domainUrn: selectedDomain.urn,
                });
                if (refetch) {
                    refetch();
                } else {
                    entityRefetch();
                }
                message.destroy();
                message.success('Successfully proposed domain. It is pending approval.');
                onModalClose();
            })
            .catch((error) => {
                handleGraphQLError({
                    error,
                    defaultMessage: 'Unable to propose domain. Something went wrong.',
                    badRequestMessage: 'Failed to propose domain. Domain is already proposed or applied.',
                    permissionMessage:
                        'Unauthorized to propose domain. The "Manage Domain Proposals" privilege is required for this asset.',
                });
                onModalClose();
            });
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
                    message.success({ content: 'Updated Domain!', duration: 2 });
                    refetch?.();
                    onModalClose();
                    setSelectedDomain(undefined);
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
            footer={
                <ModalButtonContainer>
                    <Button variant="text" color="gray" onClick={onModalClose}>
                        Cancel
                    </Button>
<<<<<<< HEAD
                    <Button
                        type="default"
                        onClick={proposeDomain}
                        disabled={!selectedDomain}
                        data-testid="propose-domain-on-entity-button"
                    >
                        Propose
                    </Button>
                    <Button type="primary" id="setDomainButton" disabled={selectedDomain === undefined} onClick={onOk}>
||||||| f14c42d2ef7
                    <Button type="primary" id="setDomainButton" disabled={selectedDomain === undefined} onClick={onOk}>
=======
                    <Button id="setDomainButton" disabled={selectedDomain === undefined} onClick={onOk}>
>>>>>>> master
                        Save
                    </Button>
                </ModalButtonContainer>
            }
            getContainer={getModalDomContainer}
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
