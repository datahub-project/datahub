import { toast } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entityV2/shared/EntityContext';
import { SetApplicationModal } from '@app/entityV2/shared/containers/profile/sidebar/Applications/SetApplicationModal';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { ApplicationLink } from '@app/sharedV2/tags/ApplicationLink';

import { useMoveApplicationMutation } from '@graphql/application.generated';
import { Application } from '@types';

const Actions = styled.div`
    display: flex;
    gap: 4px;
`;

/** The Application's parent in the app-of-apps hierarchy, with controls to set, change or clear it. */
export const SidebarParentApplicationSection = () => {
    const { t } = useTranslation('entity.shared.containers');
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const urn = useMutationUrn();
    const [moveApplicationMutation] = useMoveApplicationMutation();
    const [showModal, setShowModal] = useState(false);
    // parentApplications is ordered from the direct parent upwards.
    const parent = (entityData as Application | null)?.parentApplications?.applications?.[0];
    const canEdit = !!entityData?.privileges?.canEditProperties;

    const moveApplication = (parentApplication: string | null) =>
        moveApplicationMutation({ variables: { input: { resourceUrn: urn, parentApplication } } })
            .then(() => {
                toast.success(t('sidebar.parentApplication.setSuccess'), { duration: 2 });
                refetch?.();
            })
            .catch((e: unknown) => {
                toast.destroy();
                if (e instanceof Error) {
                    toast.error(t('sidebar.parentApplication.setFailed', { message: e.message }), { duration: 3 });
                }
            });

    return (
        <>
            <SidebarSection
                title={t('sidebar.parentApplication.sectionTitle')}
                content={
                    parent ? (
                        <ApplicationLink application={parent} fontSize={12} />
                    ) : (
                        <EmptySectionText message={t('sidebar.parentApplication.empty')} />
                    )
                }
                extra={
                    <Actions>
                        {parent && (
                            <SectionActionButton
                                dataTestId="remove-parent-application-button"
                                tip={t('sidebar.parentApplication.removeTip')}
                                icon={X}
                                onClick={(event) => {
                                    event.stopPropagation();
                                    moveApplication(null);
                                }}
                                actionPrivilege={canEdit}
                            />
                        )}
                        <SectionActionButton
                            dataTestId="set-parent-application-button"
                            icon={parent ? PencilSimple : Plus}
                            onClick={(event) => {
                                event.stopPropagation();
                                setShowModal(true);
                            }}
                            actionPrivilege={canEdit}
                        />
                    </Actions>
                }
            />
            {showModal && (
                <SetApplicationModal
                    urns={[urn]}
                    title={t('sidebar.parentApplication.modalTitle')}
                    excludeUrns={[urn]}
                    onSubmit={moveApplication}
                    onCloseModal={() => setShowModal(false)}
                />
            )}
        </>
    );
};
