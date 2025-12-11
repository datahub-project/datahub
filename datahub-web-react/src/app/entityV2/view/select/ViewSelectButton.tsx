/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useViewsSelectContext } from '@app/entityV2/view/select/ViewSelectContext';
import { renderSelectedView } from '@app/entityV2/view/select/renderSelectedView';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

export default function ViewSelectButton() {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { selectedViewName, onClear, toggleOpenState } = useViewsSelectContext();

    return renderSelectedView({ selectedViewName, onClear, isShowNavBarRedesign, onClick: toggleOpenState });
}
