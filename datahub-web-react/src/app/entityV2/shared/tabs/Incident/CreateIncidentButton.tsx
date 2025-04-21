import { PlusOutlined } from '@ant-design/icons';
import { Dropdown, message } from 'antd';
import React, { Dispatch, SetStateAction } from 'react';

import { NO_PERMISSIONS_MESSAGE } from '@app/entityV2/shared/tabs/Incident/constant';
import { CreateButton, SiblingSelectionDropdownLink } from '@app/entityV2/shared/tabs/Incident/styledComponents';
import { EntityStagedForIncident } from '@app/entityV2/shared/tabs/Incident/types';
import { useSiblingOptionsForIncidentBuilder } from '@app/entityV2/shared/tabs/Incident/utils';
import { Tooltip } from '@src/alchemy-components';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import PlatformIcon from '@src/app/sharedV2/icons/PlatformIcon';
import { EntityPrivileges } from '@src/types.generated';

type CreateIncidentButtonProps = {
    privileges: EntityPrivileges;
    setShowIncidentBuilder: Dispatch<SetStateAction<boolean>>;
    setEntity: Dispatch<SetStateAction<EntityStagedForIncident>>;
};

export const CreateIncidentButton = ({ privileges, setShowIncidentBuilder, setEntity }: CreateIncidentButtonProps) => {
    const { entityData, urn: entityUrn, entityType: dataEntityType } = useEntityData();

    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const isSiblingMode = !!entityData?.siblingsSearch?.total && !isHideSiblingMode;

    const siblingOptionsToAuthorOn = useSiblingOptionsForIncidentBuilder(entityData, entityUrn, dataEntityType) ?? [];

    const canEditIncidents = !!privileges?.canEditIncidents;

    const onCreateIncidentForEntity = ({ urn, platform, entityType }: Partial<EntityStagedForIncident>) => {
        if (!urn || !platform || !entityType) {
            console.error(`Params missing necessary data to author incidents:`, { urn, platform, entityType });
            message.error(
                `Failed to load data for the selected platform. Please contact support if this issue persists.`,
            );
            return;
        }
        if (!canEditIncidents) return;
        setEntity({
            urn,
            entityType,
            platform,
        });
        setShowIncidentBuilder(true);
    };

    const siblingSelectionOptions = siblingOptionsToAuthorOn.map((option) => ({
        key: option.title,
        label: (
            <SiblingSelectionDropdownLink
                style={{ opacity: option.disabled ? 0.5 : 1 }}
                onClick={() => onCreateIncidentForEntity(option)}
            >
                {option.platform ? (
                    <PlatformIcon platform={option.platform} size={16} styles={{ marginRight: 4 }} />
                ) : null}
                {option.title}
            </SiblingSelectionDropdownLink>
        ),
    }));

    return (
        <>
            {isSiblingMode ? (
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
                <Tooltip showArrow={false} title={!canEditIncidents ? NO_PERMISSIONS_MESSAGE : null}>
                    <CreateButton
                        onClick={() => setShowIncidentBuilder(true)}
                        disabled={!canEditIncidents}
                        data-testid="create-incident-btn-main"
                        className="create-incident-button"
                    >
                        <PlusOutlined /> Create
                    </CreateButton>
                </Tooltip>
            )}
        </>
    );
};
