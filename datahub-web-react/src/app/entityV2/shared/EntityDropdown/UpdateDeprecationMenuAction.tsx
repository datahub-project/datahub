import { Button, Tooltip, toast } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { UpdateDeprecationModal } from '@app/entityV2/shared/EntityDropdown/UpdateDeprecationModal';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useUpdateDeprecationMutation } from '@graphql/mutations.generated';

import DeprecatedIcon from '@images/deprecated-status.svg?react';

const DeprecationActionButton = styled(Button)`
    flex-shrink: 0;
    width: 28px;
    height: 28px;
    min-width: 28px;
    padding: 0;
`;

export default function UpdateDeprecationMenuAction() {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tcf } = useTranslation('common.feedback');
    const { urn, entityData, entityType } = useEntityData();
    const refetchForEntity = useRefetch();
    const [isDeprecationModalVisible, setIsDeprecationModalVisible] = useState(false);
    const entityRegistry = useEntityRegistry();
    const [updateDeprecation] = useUpdateDeprecationMutation();

    const handleUpdateDeprecation = async (deprecatedStatus: boolean) => {
        toast.loading(tcf('updating'));
        try {
            await updateDeprecation({
                variables: {
                    input: {
                        urn,
                        deprecated: deprecatedStatus,
                        note: '',
                        decommissionTime: null,
                    },
                },
            });
            toast.destroy();
            toast.success(t('deprecation.updated'), { duration: 2 });
            analytics.event({
                type: EventType.SetDeprecation,
                entityUrns: [urn],
                deprecated: deprecatedStatus,
            });
        } catch (e: unknown) {
            toast.destroy();
            if (e instanceof Error) {
                toast.error(t('deprecation.updateError', { errorMessage: e.message || '' }), { duration: 2 });
            }
        }
        refetchForEntity?.();
    };

    return (
        <Tooltip
            placement="bottom"
            title={
                !entityData?.deprecation?.deprecated
                    ? t('deprecation.markTooltip', { entityName: entityRegistry.getEntityName(entityType) })
                    : t('deprecation.markUnTooltip', { entityName: entityRegistry.getEntityName(entityType) })
            }
        >
            <DeprecationActionButton
                key="deprecation"
                variant="outline"
                color="gray"
                size="sm"
                isCircle
                onClick={() =>
                    !entityData?.deprecation?.deprecated
                        ? setIsDeprecationModalVisible(true)
                        : handleUpdateDeprecation(false)
                }
                data-testid="entity-menu-deprecate-button"
            >
                <DeprecatedIcon style={{ width: 16, height: 16 }} />
            </DeprecationActionButton>
            {isDeprecationModalVisible && (
                <UpdateDeprecationModal
                    urns={[urn]}
                    onClose={() => setIsDeprecationModalVisible(false)}
                    refetch={refetchForEntity}
                />
            )}
        </Tooltip>
    );
}
