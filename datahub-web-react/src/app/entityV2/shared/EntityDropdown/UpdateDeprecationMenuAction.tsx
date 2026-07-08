import { Tooltip, toast } from '@components';
import { Prohibit } from '@phosphor-icons/react/dist/csr/Prohibit';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import analytics, { EventType } from '@app/analytics';
import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { UpdateDeprecationModal } from '@app/entityV2/shared/EntityDropdown/UpdateDeprecationModal';
import {
    ActionMenuItem,
    ENTITY_HEADER_ACTION_ICON_SIZE,
    ENTITY_HEADER_ACTION_ICON_WEIGHT,
} from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useUpdateDeprecationMutation } from '@graphql/mutations.generated';

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
            <ActionMenuItem
                key="deprecation"
                onClick={() =>
                    !entityData?.deprecation?.deprecated
                        ? setIsDeprecationModalVisible(true)
                        : handleUpdateDeprecation(false)
                }
                data-testid="entity-menu-deprecate-button"
            >
                <Prohibit size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />
            </ActionMenuItem>
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
