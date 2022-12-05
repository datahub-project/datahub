import React from 'react';
import styled from 'styled-components';
import { DeleteOutlined, MoreOutlined } from '@ant-design/icons';
import { Col, Dropdown, List, Menu, message, Modal, Row, Tag, Tooltip, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { IconStyleType } from '../entity/Entity';
import { Domain, EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import AvatarsGroup from '../shared/avatar/AvatarsGroup';
import { getElasticCappedTotalValueText } from '../entity/shared/constants';
import { useDeleteDomainMutation } from '../../graphql/domain.generated';

const DomainItemContainer = styled(Row)`
    display: flex;
    justify-content: space-between;
    padding-left: 8px;
    padding-right: 8px;
    width: 100%;
`;

const DomainStartContainer = styled(Col)`
    display: flex;
    justify-content: start;
    align-items: center;
`;

const DomainEndContainer = styled(Col)`
    display: flex;
    justify-content: end;
    align-items: center;
`;

const DomainHeaderContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const DomainNameContainer = styled.div`
    margin-left: 16px;
    margin-right: 16px;
`;

const AvatarGroupWrapper = styled.div`
    margin-right: 10px;
`;

const MenuIcon = styled(MoreOutlined)<{ fontSize?: number }>`
    font-size: ${(props) => props.fontSize || '24'}px;
`;

type Props = {
    domain: Domain;
    onDelete?: () => void;
};

export default function DomainListItem({ domain, onDelete }: Props) {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.Domain, domain);
    const logoIcon = entityRegistry.getIcon(EntityType.Domain, 12, IconStyleType.ACCENT);
    const owners = domain.ownership?.owners;
    const totalEntitiesText = getElasticCappedTotalValueText(domain.entities?.total || 0);
    const [deleteDomainMutation] = useDeleteDomainMutation();

    const deleteDomain = () => {
        deleteDomainMutation({
            variables: {
                urn: domain.urn,
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success('Deleted Domain!');
                    onDelete?.();
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: `Failed to delee Domain!: An unknown error occurred.`, duration: 3 });
            });
    };

    const onConfirmDelete = () => {
        Modal.confirm({
            title: `Delete Domain '${displayName}'`,
            content: `Are you sure you want to remove this ${entityRegistry.getEntityName(EntityType.Domain)}?`,
            onOk() {
                deleteDomain();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <List.Item data-testid={domain.urn}>
            <DomainItemContainer>
                <DomainStartContainer>
                    <Link to={entityRegistry.getEntityUrl(EntityType.Domain, domain.urn)}>
                        <DomainHeaderContainer>
                            {logoIcon}
                            <DomainNameContainer>
                                <Typography.Text>{displayName}</Typography.Text>
                            </DomainNameContainer>
                            <Tooltip title={`There are ${totalEntitiesText} entities in this domain.`}>
                                <Tag>{totalEntitiesText} entities</Tag>
                            </Tooltip>
                        </DomainHeaderContainer>
                    </Link>
                </DomainStartContainer>
                <DomainEndContainer>
                    {owners && owners.length > 0 && (
                        <AvatarGroupWrapper>
                            <AvatarsGroup size={24} owners={owners} entityRegistry={entityRegistry} maxCount={4} />
                        </AvatarGroupWrapper>
                    )}
                    <Dropdown
                        trigger={['click']}
                        overlay={
                            <Menu>
                                <Menu.Item onClick={onConfirmDelete}>
                                    <DeleteOutlined /> &nbsp;Delete
                                </Menu.Item>
                            </Menu>
                        }
                    >
                        <MenuIcon fontSize={20} />
                    </Dropdown>
                </DomainEndContainer>
            </DomainItemContainer>
        </List.Item>
    );
}
