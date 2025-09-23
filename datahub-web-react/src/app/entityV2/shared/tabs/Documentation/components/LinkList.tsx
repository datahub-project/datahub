import { DeleteOutlined, LinkOutlined } from '@ant-design/icons';
import { colors } from '@components';
import { Pencil } from '@phosphor-icons/react';
import { Button, List, Typography, message } from 'antd';
import React, { useCallback, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { FormData, LinkFormModal } from '@app/entityV2/shared/components/styled/LinkFormModal';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { formatDateString } from '@app/entityV2/shared/containers/profile/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useRemoveLinkMutation, useUpdateLinkMutation } from '@graphql/mutations.generated';
import { InstitutionalMemoryMetadata } from '@types';

const LinkButtonsContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

const LinkListItem = styled(List.Item)`
    border-radius: 5px;
    ${LinkButtonsContainer} {
        > .ant-btn {
            opacity: 0;
        }
    }
    &:hover {
        background-color: ${ANTD_GRAY[2]};
        ${LinkButtonsContainer} {
            > .ant-btn {
                opacity: 1;
            }
        }
    }
`;

const ListOffsetIcon = styled.span`
    margin-left: -18px;
    margin-right: 6px;
`;

type LinkListProps = {
    refetch?: () => Promise<any>;
};

export const LinkList = ({ refetch }: LinkListProps) => {
    const [isEditFormModalOpened, setIsEditFormModalOpened] = useState<boolean>(false);
    const [editingMetadata, setEditingMetadata] = useState<InstitutionalMemoryMetadata>();
    const [initialValuesOfEditForm, setInitialValuesOfEditForm] = useState<FormData>();
    const { urn: entityUrn, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const [updateLinkMutation] = useUpdateLinkMutation();
    const [removeLinkMutation] = useRemoveLinkMutation();
    const links = entityData?.institutionalMemory?.elements || [];

    const handleDeleteLink = useCallback(
        async (metadata: InstitutionalMemoryMetadata) => {
            try {
                await removeLinkMutation({
                    variables: {
                        input: {
                            linkUrl: metadata.url,
                            label: metadata.label,
                            resourceUrn: metadata.associatedUrn || entityUrn,
                        },
                    },
                });
                message.success({ content: 'Link Removed', duration: 2 });
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Error removing link: \n ${e.message || ''}`, duration: 2 });
                }
            }
            refetch?.();
        },
        [refetch, removeLinkMutation, entityUrn],
    );

    const updateLink = useCallback(
        async (formData: FormData) => {
            if (!editingMetadata) return;

            try {
                await updateLinkMutation({
                    variables: {
                        input: {
                            currentLabel: editingMetadata.label || editingMetadata.description,
                            currentUrl: editingMetadata.url,
                            resourceUrn: editingMetadata.associatedUrn || entityUrn,
                            label: formData.label,
                            linkUrl: formData.url,
                            settings: {
                                showInAssetPreview: formData.showInAssetPreview,
                            },
                        },
                    },
                });
                message.success({ content: 'Link Updated', duration: 2 });
                setIsEditFormModalOpened(false);
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Error updating link: \n ${e.message || ''}`, duration: 2 });
                }
            }
            refetch?.();
        },
        [updateLinkMutation, entityUrn, editingMetadata, refetch],
    );

    const onEdit = useCallback((metadata: InstitutionalMemoryMetadata) => {
        setEditingMetadata(metadata);
        setInitialValuesOfEditForm({
            label: metadata.label || metadata.description,
            url: metadata.url,
            showInAssetPreview: !!metadata.settings?.showInAssetPreview,
        });
        setIsEditFormModalOpened(true);
    }, []);

    const onEditFormModalClosed = useCallback(() => {
        setEditingMetadata(undefined);
        setIsEditFormModalOpened(false);
        setInitialValuesOfEditForm(undefined);
    }, []);

    return entityData ? (
        <>
            {links.length > 0 && (
                <List
                    size="large"
                    dataSource={links}
                    renderItem={(link) => (
                        <LinkListItem
                            extra={
                                <LinkButtonsContainer>
                                    <Button onClick={() => onEdit(link)} type="text" shape="circle">
                                        <Pencil size={16} color={colors.gray[500]} />
                                    </Button>
                                    <Button onClick={() => handleDeleteLink(link)} type="text" shape="circle" danger>
                                        <DeleteOutlined />
                                    </Button>
                                </LinkButtonsContainer>
                            }
                        >
                            <List.Item.Meta
                                title={
                                    <Typography.Title level={5}>
                                        <a href={link.url} target="_blank" rel="noreferrer">
                                            <ListOffsetIcon>
                                                <LinkOutlined />
                                            </ListOffsetIcon>
                                            {link.description || link.label}
                                        </a>
                                    </Typography.Title>
                                }
                                description={
                                    <>
                                        Added {formatDateString(link.created.time)} by{' '}
                                        <Link to={`${entityRegistry.getEntityUrl(link.actor.type, link.actor.urn)}`}>
                                            {entityRegistry.getDisplayName(link.actor.type, link.actor)}
                                        </Link>
                                    </>
                                }
                            />
                        </LinkListItem>
                    )}
                />
            )}
            <LinkFormModal
                variant="update"
                open={isEditFormModalOpened}
                initialValues={initialValuesOfEditForm}
                onCancel={onEditFormModalClosed}
                onSubmit={updateLink}
            />
        </>
    ) : null;
};
