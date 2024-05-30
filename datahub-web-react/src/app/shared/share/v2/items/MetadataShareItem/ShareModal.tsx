import React, { useState, useMemo } from 'react';

import { Empty, Form, Modal, Select, Tag, message } from 'antd';
import styled from 'styled-components';
import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { DataHubConnection, EntityType } from '../../../../../../types.generated';
import AcrylIcon from '../../../../../../images/acryl-logo.svg?react';
import ShareIcon from '../../../../../../images/share-icon-custom.svg?react';
import { useGetSearchResultsForMultipleQuery } from '../../../../../../graphql/search.generated';
import { PLATFORM_FILTER_NAME } from '../../../../../searchV2/utils/constants';
import { PLATFORM_CONNECTION_URN } from '../../../../constants';
import { useShareEntityMutation, useUnshareEntityMutation } from '../../../../../../graphql/share.generated';
import analytics, { EventType } from '../../../../../analytics';
import { useEntityContext } from '../../../../../entity/shared/EntityContext';
import { SharedEntityInfo } from '../../../../../entityV2/shared/containers/profile/sidebar/SharedEntityInfo';
import { REDESIGN_COLORS } from '../../../../../entityV2/shared/constants';
import {
    InstanceIcon,
    StyledLabel,
} from '../../../../../entityV2/shared/containers/profile/sidebar/shared/styledComponents';
import { StyledCheckbox, StyledShareButton } from '../../styledComponents';

export const ModalTitle = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 22px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
`;

const StyledShareIcon = styled(ShareIcon)`
    height: 28px;
    width: 28px;
    path {
        stroke: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
    }
`;

const StyledContainer = styled.div`
    > div:nth-child(n + 2) {
        margin-top: 1.25rem;
    }
`;

export const StyledModal = styled(Modal)`
    font-family: Mulish;
    max-width: 480px;

    &&& .ant-modal-content {
        background-color: #eeecfa;
        box-shadow: 0px 4px 4px 0px rgba(0, 0, 0, 0.25), 0px 4px 8px 3px rgba(0, 0, 0, 0.15);
        border-radius: 12px;
    }

    .ant-modal-header {
        background-color: #eeecfa;
        border-bottom: 0;
        padding-top: 24px;
        border-radius: 12px !important;
    }

    .ant-modal-footer {
        border-top: 0;
    }

    .ant-modal-body {
        padding: 12px 24px;
    }

    .ant-modal-close-x {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
        display: flex;
        align-items: center;
        justify-content: center;
        padding-right: 9px;
        padding-top: 20px;

        :hover {
            stroke: ${REDESIGN_COLORS.TITLE_PURPLE};
        }
    }
`;

const StyledSelect = styled(Select)`
    padding-top: 8px;

    .ant-select-selector {
        border: 1px solid ${REDESIGN_COLORS.TITLE_PURPLE} !important;
        display: flex;
        align-items: center;
    }

    .ant-select-selection-placeholder {
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
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
        svg {
            font-size: 16px;
            color: ${REDESIGN_COLORS.DARK_DIVIDER};
        }
    }
`;

const ButtonContainer = styled.div`
    display: flex;
    justify-content: center;
    margin: 16px;
    height: 40px;

    .ant-btn {
        font-size: 16px;
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

const OptionsContainer = styled.div``;

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

interface Props {
    isModalVisible: boolean;
    closeModal: () => void;
}

export default function ShareModal({ isModalVisible, closeModal }: Props) {
    const [selectedInstancesToShare, setSelectedInstancesToShare] = useState<string[]>([]);
    const [selectedInstancesToUnshare, setSelectedInstancesToUnshare] = useState<string[]>([]);

    const { urn, entityData, refetch } = useEntityContext();
    const [shareEntityMutation, { loading }] = useShareEntityMutation();

    const [unshareEntityMutation] = useUnshareEntityMutation();

    const [form] = Form.useForm();

    const lastShareResults = entityData?.share?.lastShareResults;

    const handleSelectionChange = (instanceUrn: string) => {
        if (instanceUrn) {
            if (!selectedInstancesToShare.includes(instanceUrn)) {
                setSelectedInstancesToShare([...selectedInstancesToShare, instanceUrn]);
            } else {
                setSelectedInstancesToShare(selectedInstancesToShare.filter((instance) => instance !== instanceUrn));
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
                            .filter((res) => !!res.lastSuccess?.time)
                            .map((res) => res.destination.urn);
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
    const handleShare = () => {
        if (selectedInstancesToShare) {
            message.loading('Sharing entity...');
            shareEntityMutation({
                variables: {
                    input: {
                        entityUrn: urn,
                        connectionUrns: selectedInstancesToShare,
                    },
                },
            })
                .then(({ data, errors }) => {
                    message.destroy();
                    if (!errors && data?.shareEntity.succeeded) {
                        analytics.event({
                            type: EventType.SharedEntityEvent,
                            entityType: EntityType.DatahubConnection,
                            entityUrn: urn,
                            connectionUrns: selectedInstancesToShare,
                        });
                        message.success({
                            content: `Shared Entity!`,
                            duration: 3,
                        });
                        form.resetFields();
                        setSelectedInstancesToShare([]);
                        setSelectedInstancesToUnshare([]);
                        refetch();
                    } else {
                        message.error({ content: `Failed to share entity`, duration: 3 });
                    }
                })
                .catch((e) => {
                    message.destroy();
                    message.error({ content: `Failed to share entity!: \n ${e.message || ''}`, duration: 3 });
                });
        }
    };

    const handleUnshare = () => {
        if (selectedInstancesToUnshare) {
            message.loading('Unsharing entity...');
            unshareEntityMutation({
                variables: {
                    input: {
                        entityUrn: urn,
                        connectionUrns: selectedInstancesToUnshare,
                    },
                },
            })
                .then(({ data, errors }) => {
                    message.destroy();
                    if (!errors && data?.unshareEntity.succeeded) {
                        analytics.event({
                            type: EventType.UnsharedEntityEvent,
                            entityType: EntityType.DatahubConnection,
                            entityUrn: urn,
                            connectionUrns: selectedInstancesToUnshare,
                        });
                        message.success({
                            content: `Unshared Entity!`,
                            duration: 3,
                        });
                        form.resetFields();
                        setSelectedInstancesToShare([]);
                        setSelectedInstancesToUnshare([]);
                        refetch();
                    } else {
                        message.error({ content: `Failed to unshare entity`, duration: 3 });
                    }
                })
                .catch((e) => {
                    message.destroy();
                    message.error({ content: `Failed to unshare entity!: \n ${e.message || ''}`, duration: 3 });
                });
        }
    };

    const handleClose = () => {
        closeModal();
        setSelectedInstancesToShare([]);
        setSelectedInstancesToUnshare([]);
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
    const filteredResults = lastShareResults?.filter((result) => !!result.lastSuccess?.time);

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
                            selectedInstancesToUnshare={selectedInstancesToUnshare}
                            setSelectedInstancesToUnshare={setSelectedInstancesToUnshare}
                        />
                        {selectedInstancesToUnshare.length > 0 && (
                            <ButtonContainer>
                                <StyledShareButton $color={REDESIGN_COLORS.RED_ERROR} onClick={handleUnshare}>
                                    Unshare
                                </StyledShareButton>
                            </ButtonContainer>
                        )}
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
                        value={selectedInstancesToShare}
                        placeholder="Select Instances"
                        mode="multiple"
                        showSearch
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
                                            $color={REDESIGN_COLORS.TITLE_PURPLE}
                                            checked={selectedInstancesToShare.includes(option.value)}
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

            {selectedInstancesToShare.length > 0 && (
                <ButtonContainer>
                    <StyledShareButton
                        $type="filled"
                        $color={REDESIGN_COLORS.TITLE_PURPLE}
                        $hoverColor={REDESIGN_COLORS.HOVER_PURPLE}
                        onClick={handleShare}
                    >
                        Share
                    </StyledShareButton>
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
