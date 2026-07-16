import { Chart, Entity, EntityType } from '@types';

/**
 * Type guard for charts
 */
export function isChart(entity?: Entity | null | undefined): entity is Chart {
    return !!entity && entity.type === EntityType.Chart;
}
