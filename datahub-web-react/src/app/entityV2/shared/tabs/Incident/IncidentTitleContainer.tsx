import { PageTitle } from '@components';
import React, { Dispatch, SetStateAction } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { CreateIncidentButton } from '@app/entityV2/shared/tabs/Incident/CreateIncidentButton';
import { EntityStagedForIncident } from '@app/entityV2/shared/tabs/Incident/types';
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

export const IncidentTitleContainer = ({
    privileges,
    setShowIncidentBuilder,
    setEntity,
}: {
    privileges: EntityPrivileges;
    setShowIncidentBuilder: Dispatch<SetStateAction<boolean>>;
    setEntity: Dispatch<SetStateAction<EntityStagedForIncident>>;
}) => {
    const { t } = useTranslation('entity.profile.incident');
    return (
        <TitleContainer>
            <div className="left-section">
                <PageTitle variant="sectionHeader" title={t('list.title')} subTitle={t('list.subtitle')} />
            </div>
            <CreateIncidentButton
                privileges={privileges}
                setShowIncidentBuilder={setShowIncidentBuilder}
                setEntity={setEntity}
            />
        </TitleContainer>
    );
};
