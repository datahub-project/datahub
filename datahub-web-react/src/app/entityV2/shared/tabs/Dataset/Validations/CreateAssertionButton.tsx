import React from 'react';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import { useAppConfig } from '@src/app/useAppConfig';
import styled from 'styled-components';
import { Button, Dropdown, message } from 'antd';
import { Tooltip } from '@components';
import { PlusOutlined } from '@ant-design/icons';
import { DataPlatform, EntityPrivileges } from '@src/types.generated';
import PlatformIcon from '@src/app/sharedV2/icons/PlatformIcon';
import TabToolbar from '@src/app/entity/shared/components/styled/TabToolbar';
import { getColor } from '@src/alchemy-components/theme/utils';
import { useSiblingOptionsForAssertionBuilder } from './AssertionList/utils';
import { EntityStagedForAssertion } from './AssertionList/types';

const CreateButton = styled(Button)`
    &&& {
        background-color: #5c3fd1;
        height: 40px;
        color: white;
        justify-content: center;
        align-items: center;
        border-radius: 5px;
        &:disabled {
            background-color: #e0e0e0 !important;
            height: 40px;
            color: #a0a0a0;
            opacity: 0.8;
        }
    }
`;
const SiblingSelectionDropdownLink = styled.div`
    margin-bottom: 4px;
    padding: 4px 8px;
    font-size: 0.75rem;
    display: flex;
    flex-direction: row;
    align-items: center;
    color: black;
    border-radius: 8px;
    // &:hover {
    //     color: black;
    //     background-color: ${getColor('gray', 1000)};
    // }
    &:disabled {
        opacity: 0.6;
        background-color: transparent;
    }
`;

export const CreateAssertionButton = ({
    privileges,
    onCreateAssertion,
}: {
    privileges: EntityPrivileges;
    onCreateAssertion: (params: EntityStagedForAssertion) => void;
}) => {
    const primaryEntityData = useEntityData();
    const { entityData } = primaryEntityData;
    const { config } = useAppConfig();
    const assertionMonitorsEnabled = !!config?.featureFlags?.assertionMonitorsEnabled;

    const isHideSiblingMode = useIsSeparateSiblingsMode();

    const isSiblingMode = !!entityData?.siblingsSearch?.total && !isHideSiblingMode;
    const siblingOptionsToAuthorOn =
        useSiblingOptionsForAssertionBuilder(entityData, primaryEntityData.urn, primaryEntityData.entityType) ?? [];

    const noPermissionsMessage = 'You do not have permission to create an assertion for this asset';

    /* We do not enable the create button if the user does not have the privilege, OR if sibling mode is enabled */
    const canEditAssertions = privileges?.canEditAssertions || false;
    const canEditMonitors = privileges?.canEditMonitors || false;
    const isAllowedToCreateAssertion = canEditAssertions && canEditMonitors;

    const disableCreateAssertion = !isAllowedToCreateAssertion;
    const disableCreateAssertionMessage = noPermissionsMessage;

    const onCreateAssertionForEntity = (params: Partial<EntityStagedForAssertion>) => {
        if (!params.urn || !params.platform || !params.entityType) {
            message.open({
                content: `Failed to load data for the selected platform. Please contact support if this issue persists.`,
                type: 'error',
            });
            console.error(`Params missing necessary data to author assertions: ${JSON.stringify(params)}`);
            return;
        }
        onCreateAssertion({
            urn: params.urn,
            platform: params.platform,
            entityType: params.entityType,
        });
    };
    const onCreateAssertionForSiblings = () => {
        // Do nothing as dropdown will show sibling options on hover
        // We don't want to automatically open if only 1 supported platform among siblings
        // Because we want the user to understand they're explicitly authoring for that platform
    };
    const onClickCreateButton = () => {
        if (disableCreateAssertion) {
            return;
        }
        if (isSiblingMode) {
            onCreateAssertionForSiblings();
            return;
        }
        onCreateAssertionForEntity({
            ...primaryEntityData,
            platform: primaryEntityData.entityData?.platform as DataPlatform | undefined,
        });
    };

    const siblingSelectionOptions = siblingOptionsToAuthorOn.map((option) => ({
        key: option.title,
        label: (
            <Tooltip
                showArrow={false}
                placement="left"
                title={
                    option.disabled
                        ? `Native assertions are not supported for ${option.platform?.name ?? 'this platform'}.`
                        : undefined
                }
            >
                <SiblingSelectionDropdownLink
                    style={{ opacity: option.disabled ? 0.5 : 1 }}
                    onClick={() => !option.disabled && onCreateAssertionForEntity(option)}
                >
                    <PlatformIcon platform={option.platform} size={16} styles={{ marginRight: 4 }} />
                    {option.title}
                </SiblingSelectionDropdownLink>
            </Tooltip>
        ),
    }));
    const createButton = (
        <CreateButton
            onClick={onClickCreateButton}
            disabled={disableCreateAssertion}
            id="create-assertion-btn-main"
            className="create-assertion-button"
        >
            <PlusOutlined /> Create
        </CreateButton>
    );

    return assertionMonitorsEnabled ? (
        <TabToolbar style={{ boxShadow: 'none' }}>
            {isSiblingMode && !disableCreateAssertion ? (
                <Dropdown placement="bottom" menu={{ items: siblingSelectionOptions }}>
                    {createButton}
                </Dropdown>
            ) : (
                <Tooltip showArrow={false} title={disableCreateAssertion ? disableCreateAssertionMessage : null}>
                    {createButton}
                </Tooltip>
            )}
        </TabToolbar>
    ) : null;
};
