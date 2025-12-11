/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FilterRenderProps } from '@app/search/filters/render/types';

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
