import { DeleteOutlined } from '@ant-design/icons';
import { colors } from '@components';
import { Pencil } from '@phosphor-icons/react';
import { Button, List, Typography } from 'antd';
import React, { useCallback, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { EditLinkModal } from '@app/entityV2/shared/components/links/EditLinkModal';
import { LinkIcon } from '@app/entityV2/shared/components/links/LinkIcon';
import { useLinkUtils } from '@app/entityV2/shared/components/links/useLinkUtils';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { formatDateString } from '@app/entityV2/shared/containers/profile/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

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

const ListOffsetIcon = styled.div`
    display: inline-block;
    margin-left: -18px;
    margin-right: 6px;
`;

export const LinkList = () => {
    const [isEditFormModalOpened, setIsEditFormModalOpened] = useState<boolean>(false);
    const [editingMetadata, setEditingMetadata] = useState<InstitutionalMemoryMetadata>();
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const links = entityData?.institutionalMemory?.elements || [];

    const { handleDeleteLink } = useLinkUtils(editingMetadata);

    const onEdit = useCallback((metadata: InstitutionalMemoryMetadata) => {
        setEditingMetadata(metadata);
        setIsEditFormModalOpened(true);
    }, []);

    const onEditFormModalClosed = useCallback(() => {
        setEditingMetadata(undefined);
        setIsEditFormModalOpened(false);
    }, []);

    return entityData ? (
        <>
            {links.length > 0 && (
                <List
                    data-testid="link-list"
                    size="large"
                    dataSource={links}
                    renderItem={(link) => (
                        <LinkListItem
                            extra={
                                <LinkButtonsContainer>
                                    <Button
                                        onClick={() => onEdit(link)}
                                        type="text"
                                        shape="circle"
                                        data-testid="edit-link-button"
                                    >
                                        <Pencil size={16} color={colors.gray[500]} />
                                    </Button>
                                    <Button
                                        onClick={() => handleDeleteLink()}
                                        type="text"
                                        shape="circle"
                                        data-testid="remove-link-button"
                                        danger
                                    >
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
                                                <LinkIcon url={link.url} />
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
            {isEditFormModalOpened && <EditLinkModal link={editingMetadata} onClose={onEditFormModalClosed} />}
        </>
    ) : null;
};
