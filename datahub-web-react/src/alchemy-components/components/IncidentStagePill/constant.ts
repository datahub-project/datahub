import { IncidentStage } from '@src/types.generated';

export const IncidentStageLabel = {
    [IncidentStage.Triage]: 'Triage',
    [IncidentStage.Fixed]: 'Resolved',
    [IncidentStage.Investigation]: 'Investigation',
    [IncidentStage.NoActionRequired]: 'No action',
    [IncidentStage.WorkInProgress]: 'In progress',
};
