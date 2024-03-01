import React, { useState, useEffect, useMemo } from 'react';

import { Form, Modal, Select, Divider, message } from 'antd';
import styled from 'styled-components';

import { DataHubConnection, EntityType } from '../../../../../types.generated';
import { useGetSearchResultsForMultipleQuery } from '../../../../../graphql/search.generated';
import { PLATFORM_FILTER_NAME } from '../../../../search/utils/constants';
import { PLATFORM_CONNECTION_URN } from '../../../constants';
import { useShareEntityMutation } from '../../../../../graphql/share.generated';
import analytics, { EventType } from '../../../../analytics';
import { useEntityContext } from '../../../../entity/shared/EntityContext';

import { SharedEntityInfo } from '../../../../entity/shared/containers/profile/sidebar/SharedEntityInfo';

const ModalTitle = styled.span`
    font-size: 20px;
`;

const StyledLabel = styled.span`
    font-size: 14px;
    font-weight: 600;
`;

export const StyledContainer = styled.div`
    > div:nth-child(n + 2) {
        margin-top: 1.25rem;
    }
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
                        label: entity.details?.name || entity.urn,
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
        <Modal
            open={isModalVisible}
            onCancel={closeModal}
            onOk={handleSubmit}
            okButtonProps={{ disabled: isDisabled }}
            title={<ModalTitle>Send to another instance</ModalTitle>}
        >
            {filteredResults && filteredResults.length > 0 && (
                <>
                    <StyledContainer>
                        <SharedEntityInfo lastShareResults={filteredResults} showMore={false} />
                    </StyledContainer>
                    <Divider />
                </>
            )}
            <Form form={form} layout="vertical">
                <Form.Item label={<StyledLabel>Choose Instance</StyledLabel>}>
                    <Select
                        options={options}
                        loading={isLoading}
                        value={selectedInstance}
                        onChange={(option) => setSelectedInstance(option)}
                        showSearch
                        autoFocus
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
}
