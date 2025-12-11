/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { IncidentStage } from '@src/types.generated';

export const IncidentStageLabel = {
    [IncidentStage.Triage]: 'Triage',
    [IncidentStage.Fixed]: 'Fixed',
    [IncidentStage.Investigation]: 'Investigation',
    [IncidentStage.NoActionRequired]: 'No action',
    [IncidentStage.WorkInProgress]: 'In progress',
};
