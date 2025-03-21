import React, { Dispatch, SetStateAction } from 'react';
import { Tooltip, Typography } from 'antd';
import styled from 'styled-components';
import { PlusOutlined } from '@ant-design/icons';
import { EntityPrivileges } from '@src/types.generated';
import { Button, colors } from '@src/alchemy-components';

const TitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin: 20px;
    div {
        border-bottom: 0px;
    }
`;
const IncidentListTitle = styled.div`
    && {
        margin-bottom: 0px;
        font-size: 18px;
        font-weight: 700;
    }
`;

const CreateButton = styled(Button)`
    height: 40px;
`;

const SubTitle = styled(Typography.Text)`
    font-size: 14px;
    color: ${colors.gray[1700]};
`;

export const IncidentTitleContainer = ({
    privileges,
    setShowIncidentBuilder,
}: {
    privileges: EntityPrivileges;
    setShowIncidentBuilder: Dispatch<SetStateAction<boolean>>;
}) => {
    const noPermissionsMessage = 'You do not have permission to edit incidents for this asset.';

    const canEditIncidents = privileges?.canEditIncidents || false;

    return (
        <TitleContainer>
            <div className="left-section">
                <IncidentListTitle>Incidents</IncidentListTitle>
                <SubTitle>View and manage ongoing data incidents for this asset</SubTitle>
            </div>
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
        </TitleContainer>
    );
};
