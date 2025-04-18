<<<<<<< HEAD
import { FieldName, FilterComponent } from '@app/searchV2/filtersV2/types';
=======
import { FilterComponent, FieldName } from '../types';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

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
