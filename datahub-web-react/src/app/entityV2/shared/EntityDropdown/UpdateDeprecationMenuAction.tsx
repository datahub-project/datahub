import { Menu, Tooltip, toast } from '@components';
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
import { canShowEditDeprecation } from '@app/entityV2/shared/EntityDropdown/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useUpdateDeprecationMutation } from '@graphql/mutations.generated';
import { Deprecation } from '@types';

export default function UpdateDeprecationMenuAction() {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tcf } = useTranslation('common.feedback');
    const { urn, entityData, entityType } = useEntityData();
    const refetchForEntity = useRefetch();
    const [isDeprecationModalVisible, setIsDeprecationModalVisible] = useState(false);
    const [isDeprecationMenuOpen, setIsDeprecationMenuOpen] = useState(false);
    const [deprecationModalInitialValues, setDeprecationModalInitialValues] = useState<Deprecation | null>(null);
    const entityRegistry = useEntityRegistry();
    const [updateDeprecation] = useUpdateDeprecationMutation();

    const isDeprecated = !!entityData?.deprecation?.deprecated;
    const canEditDeprecation = canShowEditDeprecation(entityData?.privileges);

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
            toast.success(
                deprecatedStatus
                    ? t('deprecation.markedDeprecatedSuccess')
                    : t('deprecation.markedUnDeprecatedSuccess'),
                { duration: 2 },
            );
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

    const openDeprecationModal = (initialDeprecation: Deprecation | null) => {
        setDeprecationModalInitialValues(initialDeprecation);
        setIsDeprecationModalVisible(true);
    };

    const deprecationMenuItems = isDeprecated
        ? [
              ...(canEditDeprecation
                  ? [
                        {
                            type: 'item' as const,
                            key: 'edit-deprecation',
                            title: t('deprecation.editDeprecated'),
                            onClick: () => openDeprecationModal(entityData?.deprecation ?? null),
                        },
                    ]
                  : []),
              {
                  type: 'item' as const,
                  key: 'un-deprecate',
                  title: t('deprecation.markUnDeprecated'),
                  onClick: () => handleUpdateDeprecation(false),
              },
          ]
        : [];

    const getDeprecationTooltip = () => {
        const entityName = entityRegistry.getEntityName(entityType);
        if (!isDeprecated) {
            return t('deprecation.markTooltip', { entityName });
        }
        if (canEditDeprecation) {
            return t('deprecation.editTooltip', { entityName });
        }
        return t('deprecation.markUnTooltip', { entityName });
    };

    return (
        <>
            <Tooltip
                placement="bottom"
                title={getDeprecationTooltip()}
                open={isDeprecationMenuOpen ? false : undefined}
            >
                {isDeprecated ? (
                    <Menu
                        items={deprecationMenuItems}
                        trigger={['click']}
                        overlayStyle={{ minWidth: 180 }}
                        onOpenChange={setIsDeprecationMenuOpen}
                    >
                        <ActionMenuItem key="deprecation" data-testid="entity-menu-deprecate-button">
                            <Prohibit size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />
                        </ActionMenuItem>
                    </Menu>
                ) : (
                    <ActionMenuItem
                        key="deprecation"
                        onClick={() => openDeprecationModal(null)}
                        data-testid="entity-menu-deprecate-button"
                    >
                        <Prohibit size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />
                    </ActionMenuItem>
                )}
            </Tooltip>
            {isDeprecationModalVisible && (
                <UpdateDeprecationModal
                    urns={[urn]}
                    initialDeprecation={deprecationModalInitialValues}
                    onClose={() => {
                        setIsDeprecationModalVisible(false);
                        setDeprecationModalInitialValues(null);
                    }}
                    refetch={refetchForEntity}
                />
            )}
        </>
    );
}
