import { FilterRenderProps } from './types';

/**
 * Base interface used for custom search filter renderers
 *
 */
export interface FilterRenderer {
    /**
     * The filter field that is rendered by this renderer
     */
    field: string;

    /**
     * Renders the filter
     */
    render: (props: FilterRenderProps) => JSX.Element;

    /**
     * Ant-design icon associated with the Entity. For a list of all candidate icons, see
     * https://ant.design/components/icon/
     */
    icon: () => JSX.Element;

    /**
     * Returns a label for rendering the value of a particular field, e.g. for rendering the selected set of filters.
     * Currently only for rendering the selected value set below Search V2 top-bar.
     */
    valueLabel: (value: string) => JSX.Element;
}
