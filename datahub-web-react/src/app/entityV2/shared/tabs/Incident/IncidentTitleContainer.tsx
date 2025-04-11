import React, { Dispatch, SetStateAction } from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { EntityPrivileges } from '@src/types.generated';
import { colors } from '@src/alchemy-components';
import { EntityStagedForIncident } from './types';
import { CreateIncidentButton } from './CreateIncidentButton';

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

const SubTitle = styled(Typography.Text)`
    font-size: 14px;
    color: ${colors.gray[1700]};
`;

export const IncidentTitleContainer = ({
    privileges,
    setShowIncidentBuilder,
    setEntity,
}: {
    privileges: EntityPrivileges;
    setShowIncidentBuilder: Dispatch<SetStateAction<boolean>>;
    setEntity: Dispatch<SetStateAction<EntityStagedForIncident | undefined>>;
}) => {
    return (
        <TitleContainer>
            <div className="left-section">
                <IncidentListTitle>Incidents</IncidentListTitle>
                <SubTitle>View and manage ongoing data incidents for this asset</SubTitle>
            </div>
            <CreateIncidentButton
                privileges={privileges}
                setShowIncidentBuilder={setShowIncidentBuilder}
                setEntity={setEntity}
            />
        </TitleContainer>
    );
};
