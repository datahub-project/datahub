/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React, { Dispatch, SetStateAction } from 'react';
import styled from 'styled-components';

import { CreateIncidentButton } from '@app/entityV2/shared/tabs/Incident/CreateIncidentButton';
import { EntityStagedForIncident } from '@app/entityV2/shared/tabs/Incident/types';
import { colors } from '@src/alchemy-components';
import { EntityPrivileges } from '@src/types.generated';

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
    setEntity: Dispatch<SetStateAction<EntityStagedForIncident>>;
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
