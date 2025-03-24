import React from 'react';
import { Dropdown, message } from 'antd';

import { Tooltip } from '@src/alchemy-components';
import { PlusOutlined } from '@ant-design/icons';
import { useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import PlatformIcon from '@src/app/sharedV2/icons/PlatformIcon';

import TabToolbar from '../../components/styled/TabToolbar';
import { useSiblingOptionsForIncidentBuilder } from './utils';
import { CreateIncidentButtonProps, EntityStagedForIncident } from './types';
import { CreateButton, SiblingSelectionDropdownLink } from './styledComponents';

export const CreateIncidentButton = ({ privileges, setShowIncidentBuilder, setEntity }: CreateIncidentButtonProps) => {
    const primaryEntityData = useEntityData();
    const { entityData } = primaryEntityData;

    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const isSiblingMode = !!entityData?.siblingsSearch?.total && !isHideSiblingMode;

    const siblingOptionsToAuthorOn =
        useSiblingOptionsForIncidentBuilder(entityData, primaryEntityData.urn, primaryEntityData.entityType) ?? [];

    const noPermissionsMessage = 'You do not have permission to edit incidents for this asset.';

    const canEditIncidents = privileges?.canEditIncidents || false;

    const onCreateIncidentForEntity = ({ urn, platform, entityType }: Partial<EntityStagedForIncident>) => {
        if (!urn || !platform || !entityType) {
            console.error(`Params missing necessary data to author incidents:`, { urn, platform, entityType });
            message.error(
                `Failed to load data for the selected platform. Please contact support if this issue persists.`,
            );
            return;
        }

        setEntity({
            urn,
            entityType,
            platform,
        });

        if (canEditIncidents) {
            setShowIncidentBuilder(true);
        }
    };

    const siblingSelectionOptions = siblingOptionsToAuthorOn.map((option) => ({
        key: option.title,
        label: (
            <SiblingSelectionDropdownLink
                style={{ opacity: option.disabled ? 0.5 : 1 }}
                onClick={() => onCreateIncidentForEntity(option)}
            >
                <PlatformIcon platform={option.platform} size={16} styles={{ marginRight: 4 }} />
                {option.title}
            </SiblingSelectionDropdownLink>
        ),
    }));

    return (
        <TabToolbar style={{ boxShadow: 'none', justifyContent: 'end', padding: 0 }}>
            {isSiblingMode && canEditIncidents ? (
                <Dropdown placement="bottom" menu={{ items: siblingSelectionOptions }}>
                    <CreateButton
                        disabled={!canEditIncidents}
                        data-testid="create-incident-btn-main"
                        className="create-incident-button"
                    >
                        <PlusOutlined /> Create
                    </CreateButton>
                </Dropdown>
            ) : (
                <Tooltip showArrow={false} title={(!canEditIncidents && noPermissionsMessage) || null}>
                    <CreateButton
                        onClick={() => canEditIncidents && setShowIncidentBuilder(true)}
                        disabled={!canEditIncidents}
                        data-testid="create-incident-btn-main"
                        className="create-incident-button"
                    >
                        <PlusOutlined /> Create
                    </CreateButton>
                </Tooltip>
            )}
        </TabToolbar>
    );
};
