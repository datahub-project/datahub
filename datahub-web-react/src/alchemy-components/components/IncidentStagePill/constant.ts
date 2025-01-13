import { IncidentStage } from '@src/types.generated';

export const Incident_Stage = {
    [IncidentStage.Triage]: 'Triage',
    [IncidentStage.Fixed]: 'Resolved',
    [IncidentStage.Investigation]: 'Investigation',
    [IncidentStage.NoActionRequired]: 'No action',
    [IncidentStage.WorkInProgress]: 'In progress',
};
