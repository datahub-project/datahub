import { useViewsSelectContext } from '@app/entityV2/view/select/ViewSelectContext';
import { renderSelectedView } from '@app/entityV2/view/select/renderSelectedView';

export default function ViewSelectButton() {
    const { selectedViewName, onClear, toggleOpenState } = useViewsSelectContext();

    return renderSelectedView({ selectedViewName, onClear, onClick: toggleOpenState });
}
