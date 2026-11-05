import { TFunction } from 'i18next';

import { IncidentStage } from '@src/types.generated';

export const getIncidentStageLabel = (t: TFunction): Record<IncidentStage, string> => ({
    [IncidentStage.Triage]: t('stagePill.triage'),
    [IncidentStage.Fixed]: t('stagePill.fixed'),
    [IncidentStage.Investigation]: t('stagePill.investigation'),
    [IncidentStage.NoActionRequired]: t('stagePill.noAction'),
    [IncidentStage.WorkInProgress]: t('stagePill.inProgress'),
});
