import { useViewsSelectContext } from '@app/entityV2/view/select/ViewSelectContext';
import { renderSelectedView } from '@app/entityV2/view/select/renderSelectedView';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

export default function ViewSelectButton() {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { selectedViewName, onClear, toggleOpenState } = useViewsSelectContext();

    return renderSelectedView({ selectedViewName, onClear, isShowNavBarRedesign, onClick: toggleOpenState });
}
