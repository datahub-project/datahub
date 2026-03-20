import { Robot } from '@phosphor-icons/react/dist/csr/Robot';
import React, { useMemo, useState } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components/macro';

import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { Button, EmptyState, Modal, SimpleSelect, Text } from '@src/alchemy-components';
import { spacing } from '@src/alchemy-components/theme';

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
    gap: ${spacing.md};
`;

const OptionContent = styled.div`
    display: flex;
    flex-direction: column;
`;

const OptionDescription = styled(Text)`
    color: ${(props) => props.theme.colors.textSecondary};
`;

const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: ${spacing.xsm};
`;

const DEBOUNCE_MS = 300;

export default function SelectServiceAccountModal({ visible, onClose, onSelectServiceAccount }: Props) {
    const [selectedUrn, setSelectedUrn] = useState<string | null>(null);
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

    const serviceAccounts = useMemo(
        () => data?.listServiceAccounts?.serviceAccounts || [],
        [data?.listServiceAccounts?.serviceAccounts],
    );

    const accountsByUrn = useMemo(() => {
        const map = new Map<string, ServiceAccount>();
        serviceAccounts.forEach((acc) => map.set(acc.urn, acc));
        return map;
    }, [serviceAccounts]);

    const selectOptions = useMemo(
        () =>
            serviceAccounts.map((acc) => ({
                value: acc.urn,
                label: acc.displayName || acc.name,
            })),
        [serviceAccounts],
    );

    const handleContinue = () => {
        const account = selectedUrn ? accountsByUrn.get(selectedUrn) : null;
        if (account) {
            onSelectServiceAccount(account);
            setSelectedUrn(null);
            setSearchText('');
        }
    };

    const handleClose = () => {
        setSelectedUrn(null);
        setSearchText('');
        onClose();
    };

    useEnterKeyListener({
        querySelectorToExecuteClick: '#selectServiceAccountButton',
    });

    if (!visible) {
        return null;
    }

    const hasNoAccounts = !loading && serviceAccounts.length === 0 && !debouncedSearchText;

    return (
        <Modal
            title="Select Service Account"
            subtitle="Choose a service account to generate an API token for."
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
                        disabled={!selectedUrn}
                        data-testid="continue-select-service-account"
                    >
                        Continue
                    </Button>
                </ModalFooter>
            }
        >
            <ModalContent>
                {hasNoAccounts ? (
                    <EmptyState
                        icon={Robot}
                        title="No service accounts found"
                        description="Create a service account first."
                        size="sm"
                    />
                ) : (
                    <SimpleSelect
                        options={selectOptions}
                        values={selectedUrn ? [selectedUrn] : []}
                        onUpdate={(values) => setSelectedUrn(values.length > 0 ? values[0] : null)}
                        onClear={() => {
                            setSelectedUrn(null);
                            setSearchText('');
                        }}
                        showSearch
                        filterResultsByQuery={false}
                        onSearchChange={(value) => setSearchText(value.trim())}
                        placeholder="Search service accounts..."
                        showClear
                        width="full"
                        isLoading={loading}
                        renderCustomOptionText={(option) => {
                            const account = accountsByUrn.get(option.value);
                            return (
                                <OptionContent>
                                    <Text size="md" weight="semiBold">
                                        {option.label}
                                    </Text>
                                    {account?.description && (
                                        <OptionDescription size="sm" color="inherit">
                                            {account.description}
                                        </OptionDescription>
                                    )}
                                </OptionContent>
                            );
                        }}
                        dataTestId="select-service-account-dropdown"
                    />
                )}
            </ModalContent>
        </Modal>
    );
}
