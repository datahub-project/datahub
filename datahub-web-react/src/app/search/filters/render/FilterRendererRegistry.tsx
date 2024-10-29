import { FilterRenderer } from './FilterRenderer';
import { FilterRenderProps } from './types';

function validatedGet<K, V>(key: K, map: Map<K, V>): V {
    if (map.has(key)) {
        return map.get(key) as V;
    }
    throw new Error(`Unrecognized key ${key} provided in map ${JSON.stringify(map)}`);
}

/**
 * Serves as a singleton registry for custom filter renderers.
 */
export default class FilterRendererRegistry {
    renderers: Array<FilterRenderer> = new Array<FilterRenderer>();

    fieldNameToRenderer: Map<string, FilterRenderer> = new Map<string, FilterRenderer>();

    register(renderer: FilterRenderer) {
        this.renderers.push(renderer);
        this.fieldNameToRenderer.set(renderer.field, renderer);
    }

    hasRenderer(field: string): boolean {
        return this.fieldNameToRenderer.has(field);
    }

    render(field: string, props: FilterRenderProps): React.ReactNode {
        const renderer = validatedGet(field, this.fieldNameToRenderer);
        return renderer.render(props);
    }

    getValueLabel(field: string, value: string): React.ReactNode {
        const renderer = validatedGet(field, this.fieldNameToRenderer);
        return renderer.valueLabel(value);
    }

    getIcon(field: string): React.ReactNode {
        const renderer = validatedGet(field, this.fieldNameToRenderer);
        return renderer.icon();
    }
}
