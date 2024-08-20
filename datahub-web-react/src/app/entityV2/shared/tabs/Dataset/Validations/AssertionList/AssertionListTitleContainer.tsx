import React, { Dispatch, SetStateAction } from 'react';
import { Button, Tooltip, Typography } from 'antd';
import styled from 'styled-components';
import TabToolbar from '@src/app/entity/shared/components/styled/TabToolbar';
import { useAppConfig } from '@src/app/useAppConfig';
import { useIsSeparateSiblingsMode } from '@src/app/entity/shared/siblingUtils';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { PlusOutlined } from '@ant-design/icons';
import { EntityPrivileges } from '@src/types.generated';

const AssertionTitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
    margin: 20px;
    height: 50px;
    div {
        border-bottom: 0px;
    }
`;
const AssertionListTitle = styled(Typography.Title)`
    && {
        margin-bottom: 0px;
    }
`;

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
const SubTitle = styled(Typography.Text)`
    font-size: 11px;
    color: #5f6685;
`;
export const AssertionListTitleContainer = ({
    privileges,
    setShowAssertionBuilder,
}: {
    privileges: EntityPrivileges;
    setShowAssertionBuilder: Dispatch<SetStateAction<boolean>>;
}) => {
    const { entityData } = useEntityData();
    const { config } = useAppConfig();
    const isHideSiblingMode = useIsSeparateSiblingsMode();
    const assertionMonitorsEnabled = config?.featureFlags?.assertionMonitorsEnabled || false;

    const isSiblingMode = (entityData?.siblingsSearch?.total && !isHideSiblingMode) || false;
    const isSiblingModeMessage = (
        <>
            You cannot create an assertion for a group of assets. <br />
            <br />
            To create an assertion for a specific asset in this group, navigate to them using the <b>
                Composed Of
            </b>{' '}
            sidebar section below.
        </>
    );

    const noPermissionsMessage = 'You do not have permission to create an assertion for this asset';

    /* We do not enable the create button if the user does not have the privilege, OR if sibling mode is enabled */
    const canEditAssertions = privileges?.canEditAssertions || false;
    const canEditMonitors = privileges?.canEditMonitors || false;
    const isAllowedToCreateAssertion = canEditAssertions && canEditMonitors;

    const disableCreateAssertion = !isAllowedToCreateAssertion || isSiblingMode;
    const disableCreateAssertionMessage = isSiblingMode ? isSiblingModeMessage : noPermissionsMessage;

    return (
        <AssertionTitleContainer>
            <div className="left-section">
                <AssertionListTitle level={4}>Assertions</AssertionListTitle>
                <SubTitle>View and manage data quality checks for this table</SubTitle>
            </div>
            {assertionMonitorsEnabled && (
                <TabToolbar>
                    <Tooltip
                        showArrow={false}
                        title={(disableCreateAssertion && disableCreateAssertionMessage) || null}
                    >
                        <CreateButton
                            onClick={() => !disableCreateAssertion && setShowAssertionBuilder(true)}
                            disabled={disableCreateAssertion}
                            id="create-assertion-btn-main"
                            className="create-assertion-button"
                        >
                            <PlusOutlined /> Create
                        </CreateButton>
                    </Tooltip>
                </TabToolbar>
            )}
        </AssertionTitleContainer>
    );
};
