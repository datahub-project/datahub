import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { Empty, Form, Select, Tag, message } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useEntityContext } from '@app/entity/shared/EntityContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { SharedEntityInfo } from '@app/entityV2/shared/containers/profile/sidebar/SharedEntityInfo';
import { InstanceIcon, StyledLabel } from '@app/entityV2/shared/containers/profile/sidebar/shared/styledComponents';
import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import { PLATFORM_CONNECTION_URN } from '@app/shared/constants';
import { ModalTitle, StyledCheckbox, StyledModal } from '@app/shared/share/v2/styledComponents';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { Button } from '@src/alchemy-components';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { useShareEntityMutation, useUnshareEntityMutation } from '@graphql/share.generated';
import { DataHubConnection, EntityType, ShareLineageDirection, ShareResultState } from '@types';

import AcrylIcon from '@images/acryl-logo.svg?react';
import ShareIcon from '@images/share-icon-custom.svg?react';

const StyledShareIcon = styled(ShareIcon)`
    height: 28px;
    width: 28px;
    path {
        stroke: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
    }
`;

const StyledContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const StyledSelect = styled(Select)`
    padding-top: 8px;

    .ant-select-selector {
        border: 1px solid ${(props) => getColor('primary', 500, props.theme)} !important;
        display: flex;
        align-items: center;
    }

    .ant-select-selection-placeholder {
        color: ${(props) => getColor('primary', 500, props.theme)};
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
            color: ${REDESIGN_COLORS.DARK_DIVIDER};
        }
    }
`;

const ButtonContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    margin-top: -8px;

    .ant-btn {
        font-size: 14px;
        font-weight: 500;
    }
`;

const ActionButtons = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    align-self: end;
    gap: 16px;
    margin-top: -8px;

    .ant-btn {
        font-size: 14px;
        font-weight: 500;
    }
`;

const InstanceContainer = styled.div`
    display: flex;
    gap: 6px;
    align-items: center;
    font-size: 14px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    padding: 4px 6px;
`;

const OptionsContainer = styled.div`
    max-height: 400px;
    overflow: auto;
`;

const ListOption = styled.div`
    display: flex;
    align-items: center;
    padding: 6px 20px;
    gap: 10px;
    cursor: pointer;
    :hover {
        background: ${REDESIGN_COLORS.LIGHT_GREY};
    }
`;

const StyledTag = styled(Tag)`
    padding: 0px 7px 0px 0px;
    margin: 2px;
    display: flex;
    justify-content: start;
    align-items: center;
`;

const LineageBoxWrapper = styled.div`
    color: ${REDESIGN_COLORS.BODY_TEXT};
    margin-bottom: 16px;
    display: flex;
    gap: 8px;
    align-items: center;
    align-self: baseline;
`;

interface Props {
    isModalVisible: boolean;
    closeModal: () => void;
}

export default function ShareModal({ isModalVisible, closeModal }: Props) {
    const { theme } = useCustomTheme();
    const [instancesToShare, setInstancesToShare] = useState<string[]>([]);
    const [selectedInstances, setSelectedInstances] = useState<string[]>([]);
    const [shouldShareLineage, setShouldShareLineage] = useState(false);

    const entityRegistry = useEntityRegistryV2();
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
                        label: (
                            <InstanceContainer>
                                <InstanceIcon>
                                    <AcrylIcon />
                                </InstanceIcon>
                                {entity.details?.name || entity.urn}
                            </InstanceContainer>
                        ),
                        value: entity.urn,
                    };
                }) || []
        );
    }, [searchAcrossEntities, lastShareResults]);

    // Handle the mutation
    const handleShare = (instances, isResync) => {
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
                    } else {
                        message.error({ content: `Failed to share asset`, duration: 3 });
                    }
                })
                .catch((e) => {
                    message.error({ content: `Failed to share asset!: \n ${e.message || ''}`, duration: 3 });
                    setShouldShareLineage(false);
                });
        }
    };

    const handleUnshare = () => {
        if (selectedInstances) {
            message.loading('Unsharing asset...');
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

    const handleClose = () => {
        closeModal();
        setShouldShareLineage(false);
        setInstancesToShare([]);
        setSelectedInstances([]);
    };

    interface TagRenderProps {
        label: React.ReactNode;
        value: any;
        disabled: boolean;
        onClose: (event?: React.MouseEvent<HTMLElement, MouseEvent>) => void;
        closable: boolean;
    }

    const tagRender = ({ label, closable, onClose }: TagRenderProps) => {
        return (
            <StyledTag
                onMouseDown={(event) => {
                    event.preventDefault();
                    event.stopPropagation();
                }}
                closable={closable}
                onClose={onClose}
            >
                {label}
            </StyledTag>
        );
    };

    const isLoading = loading && searchLoading;
    const filteredResults = lastShareResults?.filter((result) => !result.implicitShareEntity);
    const implicitShares = lastShareResults?.filter((result) => !!result.implicitShareEntity);

    return (
        <StyledModal
            open={isModalVisible}
            onCancel={handleClose}
            footer={null}
            closeIcon={<CloseOutlinedIcon />}
            title={
                <ModalTitle>
                    <StyledShareIcon /> Share with an instance
                </ModalTitle>
            }
        >
            {filteredResults && filteredResults.length > 0 && (
                <>
                    <StyledContainer>
                        <SharedEntityInfo
                            lastShareResults={filteredResults}
                            selectedInstances={selectedInstances}
                            setSelectedInstances={setSelectedInstances}
                            isImplicitList={false}
                        />
                        {selectedInstances.length > 0 && (
                            <ActionButtons>
                                <Button variant="outline" color="red" onClick={handleUnshare}>
                                    Unshare
                                </Button>
                                <Button onClick={() => handleShare(selectedInstances, true)}>Resync</Button>
                            </ActionButtons>
                        )}
                    </StyledContainer>
                </>
            )}
            {implicitShares && implicitShares.length > 0 && (
                <>
                    <StyledContainer>
                        <SharedEntityInfo
                            lastShareResults={implicitShares}
                            selectedInstances={selectedInstances}
                            setSelectedInstances={setSelectedInstances}
                            isImplicitList
                        />
                    </StyledContainer>
                </>
            )}
            <Form form={form} layout="vertical">
                <Form.Item label={<StyledLabel>Share with an existing instance</StyledLabel>}>
                    <StyledSelect
                        labelInValue
                        options={options}
                        loading={isLoading}
                        tagRender={tagRender}
                        value={instancesToShare}
                        placeholder="Select Instances"
                        mode="multiple"
                        showSearch={false}
                        autoFocus
                        showArrow
                        onSelect={(option: any) => handleSelectionChange(option.value)}
                        onDeselect={(option: any) => handleSelectionChange(option.value)}
                        dropdownRender={() => (
                            <OptionsContainer>
                                {options.length === 0 && (
                                    <Empty description="No Instances" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                                )}
                                {options.map((option) => (
                                    <ListOption key={option.value} onClick={() => handleSelectionChange(option.value)}>
                                        <StyledCheckbox
                                            $color={getColor('primary', 500, theme)}
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
            </Form>

            {instancesToShare.length > 0 && (
                <ButtonContainer>
                    <LineageBoxWrapper>
                        <StyledCheckbox
                            $color={getColor('primary', 500, theme)}
                            checked={shouldShareLineage}
                            onChange={() => setShouldShareLineage(!shouldShareLineage)}
                        />
                        Share assets upstream and downstream of {entityRegistry.getDisplayName(entityType, entityData)}
                    </LineageBoxWrapper>
                    <Button onClick={() => handleShare(instancesToShare, false)}>Share</Button>
                </ButtonContainer>
            )}

            {/* Commenting this because the functionality on click is not clear, to be used later 
            <StyledLabel>Share with a new Instance</StyledLabel>
            <ButtonContainer>
                <StyledButton type="primary" ghost>
                    Create A New Instance
                </StyledButton>
        </ButtonContainer> */}
        </StyledModal>
    );
}
