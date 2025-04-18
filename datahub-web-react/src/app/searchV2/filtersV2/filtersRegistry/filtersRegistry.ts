import { FilterComponent, FieldName } from '../types';

export default class FiltersRegistry {
    registry: Map<string, FilterComponent> = new Map<FieldName, FilterComponent>();

    register(fieldName: FieldName, component: FilterComponent) {
        this.registry.set(fieldName, component);
    }

    has(fieldName: FieldName): boolean {
        return this.registry.has(fieldName);
    }

    get(fieldName: FieldName): FilterComponent | undefined {
        return this.registry.get(fieldName);
    }
}
