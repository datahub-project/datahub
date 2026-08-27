import { TFunction } from 'i18next';

import { IncidentStage } from '@src/types.generated';

export const getIncidentStageLabel = (t: TFunction): Record<IncidentStage, string> => ({
    [IncidentStage.Triage]: t('incidentStage.triage'),
    [IncidentStage.Fixed]: t('incidentStage.fixed'),
    [IncidentStage.Investigation]: t('incidentStage.investigation'),
    [IncidentStage.NoActionRequired]: t('incidentStage.noAction'),
    [IncidentStage.WorkInProgress]: t('incidentStage.inProgress'),
});
