import { Modal } from '@components';
import { message } from 'antd';
import React, { useEffect, useState } from 'react';

import { ModalButton } from '@components/components/Modal/Modal';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { useDomainsContext as useDomainsContextV2 } from '@app/domainV2/DomainsContext';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import OwnersSection, { PendingOwner } from '@app/sharedV2/owners/OwnersSection';
import { useIsNestedDomainsEnabled } from '@app/useAppConfig';

import { useCreateDomainMutation } from '@graphql/domain.generated';
import { CorpUser, Entity } from '@types';

import DomainDetailsSection from './DomainDetailsSection';
import { CreateNewDomainModalProps } from './types';

/**
 * Modal for creating a new domain with owners and optional parent domain
 */
const CreateNewDomainModal: React.FC<CreateNewDomainModalProps> = ({ onClose, open, onCreate }) => {
    const isNestedDomainsEnabled = useIsNestedDomainsEnabled();
    const { entityData } = useDomainsContextV2();
    const { user } = useUserContext();

    // Domain details state
    const [domainName, setDomainName] = useState('');
    const [domainDescription, setDomainDescription] = useState('');
    const [domainId, setDomainId] = useState('');
    const [selectedParentUrn, setSelectedParentUrn] = useState<string>(
        (isNestedDomainsEnabled && entityData?.urn) || '',
    );

    // Owners state
    const [pendingOwners, setPendingOwners] = useState<PendingOwner[]>([]);
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [placeholderOwners, setPlaceholderOwners] = useState<Entity[]>([]);

    // Loading state
    const [isLoading, setIsLoading] = useState(false);

    // Mutations
    const [createDomainMutation] = useCreateDomainMutation();

    // Set current user as placeholder owner when component mounts
    useEffect(() => {
        if (user?.urn && placeholderOwners.length === 0) {
            const currentUserEntity: CorpUser = user;
            console.log(currentUserEntity);

            setPlaceholderOwners([currentUserEntity]);
            // Automatically select the current user
            setSelectedOwnerUrns([user.urn]);
        }
    }, [user, placeholderOwners.length]);

    const onChangeOwners = (newOwners: PendingOwner[]) => {
        setPendingOwners(newOwners);
    };

    /**
     * Handler for creating the domain
     */
    const onOk = async () => {
        if (!domainName) {
            message.error('Domain name is required');
            return;
        }

        setIsLoading(true);

        try {
            // Create the domain (owners will be added automatically by the backend)
            const createDomainResult = await createDomainMutation({
                variables: {
                    input: {
                        id: domainId || undefined,
                        name: domainName.trim(),
                        description: domainDescription,
                        parentDomain: selectedParentUrn || undefined,
                    },
                },
            });

            const newDomainUrn = createDomainResult.data?.createDomain;

            if (!newDomainUrn) {
                message.error('Failed to create domain. An unexpected error occurred');
                setIsLoading(false);
                return;
            }

            // Analytics event
            analytics.event({
                type: EventType.CreateDomainEvent,
                parentDomainUrn: selectedParentUrn || undefined,
            });

            message.success(`Domain "${domainName}" successfully created`);

            // Call onCreate callback if provided
            if (onCreate) {
                onCreate(
                    newDomainUrn,
                    domainId || undefined,
                    domainName.trim(),
                    domainDescription,
                    selectedParentUrn || undefined,
                );
            }

            onClose();
            resetForm();
        } catch (e: any) {
            message.destroy();
            message.error('Failed to create domain. An unexpected error occurred');
        } finally {
            setIsLoading(false);
        }
    };

    const resetForm = () => {
        setDomainName('');
        setDomainDescription('');
        setDomainId('');
        setSelectedParentUrn((isNestedDomainsEnabled && entityData?.urn) || '');
        setPendingOwners([]);
        setSelectedOwnerUrns([]);
        setPlaceholderOwners([]);
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createNewDomainButton',
    });

    // Modal buttons configuration
    const buttons: ModalButton[] = [
        {
            text: 'Cancel',
            color: 'violet',
            variant: 'text',
            onClick: onClose,
            buttonDataTestId: 'create-domain-modal-cancel-button',
        },
        {
            text: 'Create',
            id: 'createNewDomainButton',
            color: 'violet',
            variant: 'filled',
            onClick: onOk,
            disabled: !domainName || isLoading,
            isLoading,
            buttonDataTestId: 'create-domain-modal-create-button',
        },
    ];

    return (
        <Modal title="Create New Domain" onCancel={onClose} buttons={buttons} open={open} centered width={600}>
            {/* Domain Details Section */}
            <DomainDetailsSection
                domainName={domainName}
                setDomainName={setDomainName}
                domainDescription={domainDescription}
                setDomainDescription={setDomainDescription}
                domainId={domainId}
                setDomainId={setDomainId}
                selectedParentUrn={selectedParentUrn}
                setSelectedParentUrn={setSelectedParentUrn}
                isNestedDomainsEnabled={isNestedDomainsEnabled}
            />

            {/* Owners Section */}
            <OwnersSection
                selectedOwnerUrns={selectedOwnerUrns}
                setSelectedOwnerUrns={setSelectedOwnerUrns}
                existingOwners={[]}
                onChange={onChangeOwners}
                placeholderOwners={placeholderOwners}
            />
        </Modal>
    );
};

export default CreateNewDomainModal;
