import React, { useState, useEffect, useMemo } from 'react';
import { Form, Modal, Select, Divider, message, Empty, Checkbox, Button } from 'antd';
import styled from 'styled-components';
import { DataHubConnection, EntityType, ShareLineageDirection, ShareResultState } from '../../../../../types.generated';
import { useGetSearchResultsForMultipleQuery } from '../../../../../graphql/search.generated';
import { PLATFORM_FILTER_NAME } from '../../../../search/utils/constants';
import { PLATFORM_CONNECTION_URN } from '../../../constants';
import { useShareEntityMutation, useUnshareEntityMutation } from '../../../../../graphql/share.generated';
import analytics, { EventType } from '../../../../analytics';
import { useEntityContext } from '../../../../entity/shared/EntityContext';
import { SharedEntityInfo } from '../../../../entity/shared/containers/profile/sidebar/SharedEntityInfo';
import { ANTD_GRAY_V2, REDESIGN_COLORS } from '../../../../entity/shared/constants';
import { useEntityRegistry } from '../../../../useEntityRegistry';

const ModalTitle = styled.span`
    font-size: 20px;
`;

const StyledLabel = styled.span`
    font-size: 14px;
    font-weight: 600;
`;

const StyledSelect = styled(Select)`
    padding-top: 8px;

    .ant-select-selector {
        display: flex;
        align-items: center;
    }

    .ant-select-selection-placeholder {
        font-size: 14px;
        font-weight: 500;
    }

    .ant-select-selection-search-input {
        height: 38px !important;
    }

    .ant-select-selection-item {
        display: flex;
        align-items: center;
    }

    .ant-select-arrow {
        height: 16px;
        svg {
            font-size: 16px;
        }
    }
`;

const ListOption = styled.div`
    display: flex;
    align-items: center;
    padding: 6px 20px;
    gap: 10px;
    cursor: pointer;
    :hover {
        background: ${REDESIGN_COLORS.BLUE};
    }
`;

export const StyledContainer = styled.div`
    > div:nth-child(n + 2) {
        margin-top: 1.25rem;
    }
`;

const ButtonContainer = styled.div`
    display: flex;
    justify-content: end;
    gap: 8px;

    .ant-btn {
        font-weight: 500;
    }
`;

const LineageBoxWrapper = styled.div`
    color: ${ANTD_GRAY_V2[10]};
    display: flex;
    gap: 8px;
    align-items: center;
    align-self: baseline;
`;

const OptionsContainer = styled.div`
    max-height: 400px;
    overflow: auto;
`;

interface Props {
    isModalVisible: boolean;
    closeModal: () => void;
}

export default function ShareModal({ isModalVisible, closeModal }: Props) {
    const [instancesToShare, setInstancesToShare] = useState<string[]>([]);
    const [selectedInstances, setSelectedInstances] = useState<string[]>([]);
    const [shouldShareLineage, setShouldShareLineage] = useState(false);

    const entityRegistry = useEntityRegistry();
    const { urn, entityType, entityData, refetch } = useEntityContext();
    const [shareEntityMutation, { loading }] = useShareEntityMutation();
    const [unshareEntityMutation] = useUnshareEntityMutation();

    const [form] = Form.useForm();

    const lastShareResults = entityData?.share?.lastShareResults;

    const handleSelectionChange = (instanceUrn: string) => {
        if (instanceUrn) {
            if (!instancesToShare.includes(instanceUrn)) {
                setInstancesToShare([...instancesToShare, instanceUrn]);
            } else {
                setInstancesToShare(instancesToShare.filter((instance) => instance !== instanceUrn));
            }
        }
    };

    // Execute search
    const { data: searchData, loading: searchLoading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.DatahubConnection],
                query: '*',
                start: 0,
                count: 50,
                orFilters: [{ and: [{ field: PLATFORM_FILTER_NAME, values: [PLATFORM_CONNECTION_URN] }] }],
                searchFlags: { skipCache: true },
            },
        },
    });

    // Format options
    const searchAcrossEntities = searchData?.searchAcrossEntities;

    // Format the options
    const options = useMemo(() => {
        return (
            searchAcrossEntities?.searchResults
                .filter((result) => {
                    if (lastShareResults) {
                        // Filter connections that are already shared
                        const resultUrn = result.entity.urn;
                        const sharedUrns = lastShareResults!
                            .filter((res) => !!res.lastSuccess?.time && res.destination)
                            .map((res) => res.destination?.urn);
                        return !sharedUrns.includes(resultUrn);
                    }
                    return true;
                })
                .map((result) => {
                    const entity = result.entity as DataHubConnection;
                    return {
                        label: entity.details?.name || entity.urn,
                        value: entity.urn,
                    };
                }) || []
        );
    }, [searchAcrossEntities, lastShareResults]);

    // Set selected if only 1 option returns in list
    useEffect(() => {
        if (options.length === 1) setInstancesToShare([options[0].value]);
    }, [options]);

    const handleUnshare = () => {
        if (selectedInstances) {
            message.loading('Unsharing entity...');
            unshareEntityMutation({
                variables: {
                    input: {
                        entityUrn: urn,
                        connectionUrns: selectedInstances,
                    },
                },
            })
                .then(({ data, errors }) => {
                    message.destroy();
                    const unshareResult = data?.unshareEntity.share.lastUnshareResults?.filter((result) =>
                        selectedInstances.includes(result.destination?.urn || ''),
                    )[0];
                    if (!errors && !(unshareResult?.status === ShareResultState.Failure)) {
                        analytics.event({
                            type: EventType.UnsharedEntityEvent,
                            entityType: EntityType.DatahubConnection,
                            entityUrn: urn,
                            connectionUrns: selectedInstances,
                        });
                        if (unshareResult?.status === ShareResultState.Success) {
                            message.success({
                                content: `Unshared Asset!`,
                                duration: 3,
                            });
                        } else if (unshareResult?.status === ShareResultState.Running) {
                            message.success({
                                content: `Asset unsharing in progress!`,
                                duration: 3,
                            });
                        }
                        form.resetFields();
                        setInstancesToShare([]);
                        setSelectedInstances([]);
                        refetch();
                    } else {
                        message.error({ content: `Failed to unshare asset`, duration: 3 });
                    }
                })
                .catch((e) => {
                    message.destroy();
                    message.error({ content: `Failed to unshare asset!: \n ${e.message || ''}`, duration: 3 });
                });
        }
    };

    // Handle the mutation
    const handleSubmit = (instances, isResync) => {
        if (instances) {
            message.loading('Sharing asset...');
            shareEntityMutation({
                variables: {
                    input: {
                        entityUrn: urn,
                        connectionUrns: instances,
                        lineageDirection: !isResync && shouldShareLineage ? ShareLineageDirection.Both : undefined,
                    },
                },
            })
                .then(({ data, errors }) => {
                    message.destroy();
                    const shareResult = data?.shareEntity.share.lastShareResults.filter((result) =>
                        instances.includes(result.destination?.urn || ''),
                    )[0];
                    if (!errors && !(shareResult?.status === ShareResultState.Failure)) {
                        analytics.event({
                            type: EventType.SharedEntityEvent,
                            entityType: EntityType.DatahubConnection,
                            entityUrn: urn,
                            connectionUrns: instances,
                        });
                        if (shareResult?.status === ShareResultState.Success) {
                            message.success({
                                content: `Shared Asset!`,
                                duration: 3,
                            });
                        } else if (shareResult?.status === ShareResultState.Running) {
                            message.success({
                                content: `Asset sharing in progress!`,
                                duration: 3,
                            });
                        }
                        form.resetFields();
                        setInstancesToShare([]);
                        setSelectedInstances([]);
                        refetch();
                        closeModal();
                    } else {
                        message.error({ content: `Failed to share asset`, duration: 3 });
                    }
                })
                .catch((e) => {
                    message.destroy();
                    message.error({ content: `Failed to share asset!: \n ${e.message || ''}`, duration: 3 });
                });
        }
    };

    const isLoading = loading && searchLoading;
    const isDisabled = isLoading || !instancesToShare.length;
    const filteredResults = lastShareResults?.filter((result) => !result.implicitShareEntity);
    const implicitShares = lastShareResults?.filter((result) => !!result.implicitShareEntity);

    const handleClose = () => {
        setInstancesToShare([]);
        setSelectedInstances([]);
        closeModal();
    };

    return (
        <Modal
            open={isModalVisible}
            onCancel={handleClose}
            onOk={() => handleSubmit(instancesToShare, false)}
            okButtonProps={{ disabled: isDisabled }}
            title={<ModalTitle>Send to another instance</ModalTitle>}
            width={550}
        >
            {filteredResults && filteredResults.length > 0 && (
                <>
                    <StyledContainer>
                        <SharedEntityInfo
                            selectedInstances={selectedInstances}
                            setSelectedInstances={setSelectedInstances}
                            lastShareResults={filteredResults}
                            showSelectMode
                            isImplicitList={false}
                        />
                        {selectedInstances.length > 0 && (
                            <ButtonContainer>
                                <Button type="primary" onClick={handleUnshare}>
                                    Unshare
                                </Button>
                                <Button onClick={() => handleSubmit(selectedInstances, true)}>Resync</Button>
                            </ButtonContainer>
                        )}
                    </StyledContainer>
                    <Divider />
                </>
            )}
            {implicitShares && implicitShares.length > 0 && (
                <>
                    <StyledContainer>
                        <SharedEntityInfo
                            selectedInstances={selectedInstances}
                            setSelectedInstances={setSelectedInstances}
                            lastShareResults={implicitShares}
                            showMore
                            showSelectMode
                            isImplicitList
                        />
                    </StyledContainer>
                    <Divider />
                </>
            )}
            <Form form={form} layout="vertical">
                <Form.Item label={<StyledLabel>Choose Instance</StyledLabel>}>
                    <StyledSelect
                        labelInValue
                        options={options}
                        loading={isLoading}
                        value={instancesToShare}
                        placeholder="Select Instances"
                        onSelect={(option: any) => handleSelectionChange(option.value)}
                        onDeselect={(option: any) => handleSelectionChange(option.value)}
                        showSearch={false}
                        autoFocus
                        showArrow
                        mode="multiple"
                        dropdownRender={() => (
                            <OptionsContainer>
                                {options.length === 0 && (
                                    <Empty description="No Instances" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                                )}
                                {options.map((option) => (
                                    <ListOption key={option.value} onClick={() => handleSelectionChange(option.value)}>
                                        <Checkbox
                                            checked={instancesToShare.includes(option.value)}
                                            onChange={() => handleSelectionChange(option.value)}
                                        />
                                        {option.label}
                                    </ListOption>
                                ))}
                            </OptionsContainer>
                        )}
                    />
                </Form.Item>
                {instancesToShare.length > 0 && (
                    <LineageBoxWrapper>
                        <Checkbox
                            checked={shouldShareLineage}
                            onChange={() => setShouldShareLineage(!shouldShareLineage)}
                        />
                        Share assets upstream and downstream of {entityRegistry.getDisplayName(entityType, entityData)}
                    </LineageBoxWrapper>
                )}
            </Form>
        </Modal>
    );
}
