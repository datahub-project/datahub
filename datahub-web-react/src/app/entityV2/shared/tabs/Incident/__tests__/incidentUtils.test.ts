import { INCIDENT_DISPLAY_TYPES } from '@app/entityV2/shared/tabs/Incident/incidentUtils';

import { IncidentType } from '@types';

// Locks the CURRENT (pre-i18n-refactor) English display names produced by the
// module-scope INCIDENT_DISPLAY_TYPES constant. The upcoming refactor converts
// these into i18next getters/factories; the resolved English strings and the
// type->name mapping must stay identical.
describe('INCIDENT_DISPLAY_TYPES display names', () => {
    test('maps incident types to their English display names', () => {
        const byType = Object.fromEntries(INCIDENT_DISPLAY_TYPES.map((entry) => [entry.type, entry.name]));

        expect(byType[IncidentType.Operational]).toBe('Operational');
        expect(byType.OTHER).toBe('Custom');
    });
});
