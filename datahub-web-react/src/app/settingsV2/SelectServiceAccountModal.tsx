import { Empty, Modal, Typography } from 'antd';
import React, { useState } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import CustomAvatar from '@app/shared/avatar/CustomAvatar';
import { ModalButtonContainer } from '@app/shared/button/styledComponents';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { Button, Input, Loader } from '@src/alchemy-components';

import { useListServiceAccountsQuery } from '@graphql/auth.generated';
import { ServiceAccount } from '@types';

type Props = {
    visible: boolean;
    onClose: () => void;
    onSelectServiceAccount: (serviceAccount: ServiceAccount) => void;
};

const ServiceAccountList = styled.div`
    max-height: 400px;
    overflow-y: auto;
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 8px;
`;

const ServiceAccountItem = styled.div<{ $isSelected: boolean }>`
    display: flex;
    align-items: center;
    padding: 12px 16px;
    cursor: pointer;
    border-bottom: 1px solid ${ANTD_GRAY[4]};

    &:last-child {
        border-bottom: none;
    }

    &:hover {
        background-color: ${ANTD_GRAY[3]};
    }

    ${(props) =>
        props.$isSelected &&
        `
        background-color: ${ANTD_GRAY[4]};
    `}
`;

const ServiceAccountInfo = styled.div`
    margin-left: 12px;
    display: flex;
    flex-direction: column;
`;

const ServiceAccountName = styled(Typography.Text)`
    font-weight: 600;
`;

const ServiceAccountId = styled(Typography.Text)`
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

const LoadingContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 60px;
    min-height: 200px;
`;

const DescriptionText = styled(Typography.Paragraph)`
    margin-bottom: 16px;
`;

const SearchInputWrapper = styled.div`
    margin-bottom: 12px;
`;

const DEBOUNCE_MS = 300;

export default function SelectServiceAccountModal({ visible, onClose, onSelectServiceAccount }: Props) {
    const [selectedAccount, setSelectedAccount] = useState<ServiceAccount | null>(null);
    const [searchText, setSearchText] = useState('');
    const [debouncedSearchText, setDebouncedSearchText] = useState('');

    // Debounce the search text for server-side search
    useDebounce(() => setDebouncedSearchText(searchText), DEBOUNCE_MS, [searchText]);

    const { data, loading } = useListServiceAccountsQuery({
        variables: {
            input: {
                start: 0,
                count: 10,
                query: debouncedSearchText || undefined,
            },
        },
        skip: !visible,
        fetchPolicy: 'cache-and-network',
    });

    const serviceAccounts = data?.listServiceAccounts?.serviceAccounts || [];

    const handleContinue = () => {
        if (selectedAccount) {
            onSelectServiceAccount(selectedAccount);
            setSelectedAccount(null);
        }
    };

    const handleClose = () => {
        setSelectedAccount(null);
        setSearchText('');
        onClose();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#selectServiceAccountButton',
    });

    return (
        <Modal
            title="Select Service Account"
            visible={visible}
            onCancel={handleClose}
            width={500}
            footer={
                <ModalButtonContainer>
                    <Button
                        onClick={handleClose}
                        variant="text"
                        color="gray"
                        data-testid="cancel-select-service-account"
                    >
                        Cancel
                    </Button>
                    <Button
                        id="selectServiceAccountButton"
                        onClick={handleContinue}
                        disabled={!selectedAccount}
                        data-testid="continue-select-service-account"
                    >
                        Continue
                    </Button>
                </ModalButtonContainer>
            }
        >
            <DescriptionText type="secondary">
                Select a service account to create an API token for. The token will be associated with this service
                account and can be used for programmatic access to DataHub APIs.
            </DescriptionText>
            <SearchInputWrapper>
                <Input
                    label=""
                    placeholder="Search service accounts..."
                    icon={{ icon: 'Search', source: 'material' }}
                    value={searchText}
                    setValue={setSearchText}
                    inputTestId="search-service-accounts-input"
                />
            </SearchInputWrapper>
            {loading && (
                <LoadingContainer>
                    <Loader />
                </LoadingContainer>
            )}
            {!loading && serviceAccounts.length === 0 && !debouncedSearchText && (
                <Empty
                    description="No service accounts found. Create a service account first."
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                />
            )}
            {!loading && serviceAccounts.length === 0 && debouncedSearchText && (
                <Empty
                    description={`No service accounts match "${debouncedSearchText}"`}
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                />
            )}
            {!loading && serviceAccounts.length > 0 && (
                <ServiceAccountList>
                    {serviceAccounts.map((account) => {
                        const displayName = account.displayName || account.name;
                        const isSelected = selectedAccount?.urn === account.urn;
                        return (
                            <ServiceAccountItem
                                key={account.urn}
                                $isSelected={isSelected}
                                onClick={() => setSelectedAccount(account)}
                                data-testid={`service-account-option-${account.name}`}
                            >
                                <CustomAvatar size={32} name={displayName} isGroup={false} />
                                <ServiceAccountInfo>
                                    <ServiceAccountName>{displayName}</ServiceAccountName>
                                    <ServiceAccountId>{account.name}</ServiceAccountId>
                                </ServiceAccountInfo>
                            </ServiceAccountItem>
                        );
                    })}
                </ServiceAccountList>
            )}
        </Modal>
    );
}
