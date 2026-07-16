import { Dashboard, Entity, EntityType } from '@types';

/**
 * Type guard for dashboards
 */
export function isDashboard(entity?: Entity | null | undefined): entity is Dashboard {
    return !!entity && entity.type === EntityType.Dashboard;
}
