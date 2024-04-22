import React, { useState, useEffect, useMemo } from 'react';

import { Form, Modal, Select, message } from 'antd';
import styled from 'styled-components';
import CloseOutlinedIcon from '@mui/icons-material/CloseOutlined';
import { DataHubConnection, EntityType } from '../../../../../../types.generated';
import AcrylIcon from '../../../../../../images/acryl-logo.svg?react';
import ShareIcon from '../../../../../../images/share-icon-custom.svg?react';
import { useGetSearchResultsForMultipleQuery } from '../../../../../../graphql/search.generated';
import { PLATFORM_FILTER_NAME } from '../../../../../searchV2/utils/constants';
import { PLATFORM_CONNECTION_URN } from '../../../../constants';
import { useShareEntityMutation } from '../../../../../../graphql/share.generated';
import analytics, { EventType } from '../../../../../analytics';
import { useEntityContext } from '../../../../../entity/shared/EntityContext';
import { SharedEntityInfo } from '../../../../../entityV2/shared/containers/profile/sidebar/SharedEntityInfo';
import { REDESIGN_COLORS } from '../../../../../entityV2/shared/constants';
import {
    InstanceIcon,
    StyledLabel,
} from '../../../../../entityV2/shared/containers/profile/sidebar/shared/styledComponents';

const ModalTitle = styled.span`
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

const StyledModal = styled(Modal)`
    font-family: Mulish;
    max-width: 460px;

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
    .ant-select-selector {
        height: 40px !important;
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

// Commenting to be used later

/* const ButtonContainer = styled.div`
    display: flex;
    justify-content: center;
    margin: 16px;
`;

const StyledButton = styled(Button)`
    && {
        border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }
`;
*/

const InstanceContainer = styled.div`
    display: flex;
    gap: 5px;
    align-items: center;
    font-size: 14px;
    font-weight: 500;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    padding: 5px;
`;

interface Props {
    isModalVisible: boolean;
    closeModal: () => void;
}

export default function ShareModal({ isModalVisible, closeModal }: Props) {
    const [selectedInstance, setSelectedInstance] = useState<string>();
    const { urn, entityData, refetch } = useEntityContext();
    const [shareEntityMutation, { loading }] = useShareEntityMutation();
    const [form] = Form.useForm();

    const lastShareResults = entityData?.share?.lastShareResults;

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

    // Set selected if only 1 option returns in list
    useEffect(() => {
        if (options.length === 1) setSelectedInstance(options[0].value);
    }, [options]);

    // Handle the mutation
    const handleSubmit = () => {
        if (selectedInstance) {
            message.loading('Sharing entity...');
            shareEntityMutation({
                variables: {
                    input: {
                        entityUrn: urn,
                        connectionUrn: selectedInstance,
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
                            connectionUrn: selectedInstance,
                        });
                        message.success({
                            content: `Shared Entity!`,
                            duration: 3,
                        });
                        form.resetFields();
                        setSelectedInstance(undefined);
                        refetch();
                        closeModal();
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

    const isLoading = loading && searchLoading;
    const isDisabled = isLoading || !selectedInstance;
    const filteredResults = lastShareResults?.filter((result) => !!result.lastSuccess?.time);

    return (
        <StyledModal
            open={isModalVisible}
            onCancel={closeModal}
            onOk={handleSubmit}
            okButtonProps={{ disabled: isDisabled }}
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
                        <SharedEntityInfo lastShareResults={filteredResults} />
                    </StyledContainer>
                </>
            )}
            <Form form={form} layout="vertical">
                <Form.Item label={<StyledLabel>Share with an existing instance</StyledLabel>}>
                    <StyledSelect
                        options={options}
                        loading={isLoading}
                        value={selectedInstance}
                        onChange={(option: any) => setSelectedInstance(option)}
                        placeholder="Select Instances"
                        showSearch
                        autoFocus
                    />
                </Form.Item>
            </Form>

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
