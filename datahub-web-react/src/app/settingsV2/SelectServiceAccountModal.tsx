import { Empty } from 'antd';
import React, { useState } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components/macro';

import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { Button, Input, Loader, Modal, Table, Text } from '@src/alchemy-components';
import { Column } from '@src/alchemy-components/components/Table';
import { colors } from '@src/alchemy-components/theme';

import { useListServiceAccountsQuery } from '@graphql/auth.generated';
import { ServiceAccount } from '@types';

type Props = {
    visible: boolean;
    onClose: () => void;
    onSelectServiceAccount: (serviceAccount: ServiceAccount) => void;
};

const ModalContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const DescriptionText = styled(Text)`
    color: ${colors.gray[1700]};
`;

const LoadingContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 60px;
    min-height: 200px;
`;

const TableWrapper = styled.div`
    border: 1px solid ${colors.gray[200]};
    border-radius: 8px;
    overflow: hidden;
    max-height: 350px;
    overflow-y: auto;

    .selected-row {
        background-color: ${colors.violet[0]} !important;
    }

    tr {
        cursor: pointer;
    }
`;

const ServiceAccountDetails = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

const DEBOUNCE_MS = 300;

export default function SelectServiceAccountModal({ visible, onClose, onSelectServiceAccount }: Props) {
    const [selectedAccount, setSelectedAccount] = useState<ServiceAccount | null>(null);
    const [searchText, setSearchText] = useState('');
    const [debouncedSearchText, setDebouncedSearchText] = useState('');

    useDebounce(() => setDebouncedSearchText(searchText), DEBOUNCE_MS, [searchText]);

    const { data, loading } = useListServiceAccountsQuery({
        variables: {
            input: {
                start: 0,
                count: 20,
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
            setSearchText('');
        }
    };

    const handleClose = () => {
        setSelectedAccount(null);
        setSearchText('');
        onClose();
    };

    useEnterKeyListener({
        querySelectorToExecuteClick: '#selectServiceAccountButton',
    });

    const columns: Column<ServiceAccount>[] = [
        {
            title: 'Name',
            key: 'name',
            render: (record: ServiceAccount) => {
                const displayName = record.displayName || record.name;
                return (
                    <ServiceAccountDetails>
                        <Text size="md" weight="semiBold">
                            {displayName}
                        </Text>
                        {record.description && (
                            <Text size="sm" color="gray">
                                {record.description}
                            </Text>
                        )}
                    </ServiceAccountDetails>
                );
            },
        },
    ];

    if (!visible) {
        return null;
    }

    return (
        <Modal
            title="Select Service Account"
            onCancel={handleClose}
            width={500}
            dataTestId="select-service-account-modal"
            footer={
                <ModalFooter>
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
                </ModalFooter>
            }
        >
            <ModalContent>
                <DescriptionText size="sm">
                    Select a service account to create an API token for. The token will be associated with this service
                    account and can be used for programmatic access to DataHub APIs.
                </DescriptionText>
                <Input
                    label=""
                    placeholder="Search service accounts..."
                    icon={{ icon: 'Search', source: 'material' }}
                    value={searchText}
                    setValue={setSearchText}
                    inputTestId="search-service-accounts-input"
                />
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
                    <TableWrapper>
                        <Table
                            columns={columns}
                            data={serviceAccounts}
                            showHeader
                            isBorderless
                            onRowClick={(record: ServiceAccount) => setSelectedAccount(record)}
                            rowClassName={(record: ServiceAccount) =>
                                selectedAccount?.urn === record.urn ? 'selected-row' : ''
                            }
                            rowDataTestId={(record: ServiceAccount) => `service-account-option-${record.name}`}
                        />
                    </TableWrapper>
                )}
            </ModalContent>
        </Modal>
    );
}
