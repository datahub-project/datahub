import MoreVertIcon from '@mui/icons-material/MoreVert';
import { Dropdown, List, Modal, Typography, message } from 'antd';
import React, { useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import CreateServiceAccountTokenModal from '@app/identity/serviceAccount/CreateServiceAccountTokenModal';
import SelectRole from '@app/identity/user/SelectRole';
import CustomAvatar from '@app/shared/avatar/CustomAvatar';
import { Text } from '@src/alchemy-components';

import { DataHubRole, ServiceAccount } from '@types';

type Props = {
    serviceAccount: ServiceAccount;
    selectRoleOptions: Array<DataHubRole>;
    onDelete: () => void;
    onCreateToken: () => void;
    refetch?: () => void;
};

const ServiceAccountItemContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding-left: 8px;
    padding-right: 8px;
    width: 100%;
`;

const ServiceAccountHeaderContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const ServiceAccountInfo = styled.div`
    margin-left: 16px;
    margin-right: 20px;
`;

const ButtonGroup = styled.div`
    display: flex;
    justify-content: space-evenly;
    align-items: center;
    gap: 8px;
`;

const MenuButton = styled(MoreVertIcon)`
    width: 20px;
    color: ${ANTD_GRAY[8]};
    &&& {
        padding-left: 0px;
        padding-right: 0px;
        font-size: 18px;
    }
    :hover {
        cursor: pointer;
    }
`;

const DropdownContentWrapper = styled.div`
    background-color: white;
    border-radius: 8px;
    box-shadow: 0px 0px 14px 0px rgba(0, 0, 0, 0.15);
    padding: 8px;
`;

const DropdownOption = styled.div`
    padding: 8px;
    border-radius: 8px;
    cursor: pointer;

    &:hover {
        background: linear-gradient(
            180deg,
            rgba(243, 244, 246, 0.5) -3.99%,
            rgba(235, 236, 240, 0.5) 53.04%,
            rgba(235, 236, 240, 0.5) 100%
        );
    }
`;

export default function ServiceAccountListItem({
    serviceAccount,
    selectRoleOptions,
    onDelete,
    onCreateToken,
    refetch,
}: Props) {
    const history = useHistory();
    const [isCreatingToken, setIsCreatingToken] = useState(false);

    const displayName = serviceAccount.displayName || serviceAccount.name;

    // Extract current role from relationships
    const castedServiceAccount = serviceAccount as any;
    const roleRelationships = castedServiceAccount?.roles?.relationships;
    const currentRole =
        roleRelationships && roleRelationships.length > 0 && (roleRelationships[0]?.entity as DataHubRole);
    const currentRoleUrn = currentRole && currentRole.urn;

    const handleDeleteClick = () => {
        Modal.confirm({
            title: 'Delete Service Account',
            content: `Are you sure you want to delete the service account "${displayName}"? This action cannot be undone and will revoke all associated API tokens.`,
            okText: 'Delete',
            okType: 'danger',
            cancelText: 'Cancel',
            onOk: onDelete,
        });
    };

    const handleCreateTokenClick = () => {
        setIsCreatingToken(true);
    };

    const handleCopyUrn = () => {
        navigator.clipboard.writeText(serviceAccount.urn);
        message.success('Copied URN to clipboard');
    };

    const handleTokenCreated = () => {
        setIsCreatingToken(false);
        onCreateToken();
        // Navigate to the tokens page
        history.push('/settings/tokens');
    };

    const handleDropdownClick = (e) => {
        e.stopPropagation();
    };

    return (
        <List.Item>
            <ServiceAccountItemContainer>
                <ServiceAccountHeaderContainer>
                    <CustomAvatar size={32} name={displayName} isGroup={false} />
                    <ServiceAccountInfo>
                        <div>
                            <Typography.Text strong>{displayName}</Typography.Text>
                        </div>
                        {serviceAccount.description && (
                            <div>
                                <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                                    {serviceAccount.description}
                                </Typography.Text>
                            </div>
                        )}
                    </ServiceAccountInfo>
                </ServiceAccountHeaderContainer>
            </ServiceAccountItemContainer>
            <ButtonGroup>
                <SelectRole
                    user={{ urn: serviceAccount.urn, username: serviceAccount.name, type: serviceAccount.type } as any}
                    userRoleUrn={currentRoleUrn || ''}
                    selectRoleOptions={selectRoleOptions}
                    refetch={refetch}
                />
                <Dropdown
                    dropdownRender={() => (
                        <DropdownContentWrapper>
                            <DropdownOption
                                role="menuitem"
                                aria-label="Create Token"
                                onClick={handleCreateTokenClick}
                                data-testid="create-token-menu-item"
                            >
                                <Text>Create Token</Text>
                            </DropdownOption>
                            <DropdownOption
                                role="menuitem"
                                aria-label="Copy URN"
                                onClick={handleCopyUrn}
                                data-testid="copy-urn-menu-item"
                            >
                                <Text>Copy URN</Text>
                            </DropdownOption>
                            <DropdownOption
                                role="menuitem"
                                aria-label="Delete"
                                onClick={handleDeleteClick}
                                data-testid="delete-service-account-menu-item"
                            >
                                <Text color="red">Delete</Text>
                            </DropdownOption>
                        </DropdownContentWrapper>
                    )}
                    trigger={['click']}
                >
                    <MenuButton
                        data-testid={`service-account-menu-${serviceAccount.name}`}
                        onClick={handleDropdownClick}
                    />
                </Dropdown>
            </ButtonGroup>
            <CreateServiceAccountTokenModal
                visible={isCreatingToken}
                serviceAccount={serviceAccount}
                onClose={() => setIsCreatingToken(false)}
                onCreateToken={handleTokenCreated}
            />
        </List.Item>
    );
}
