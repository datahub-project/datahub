import { useMemo } from 'react';
import { SelectOption, SelectLabelDisplayProps } from '../../types';
import MultiSelectDefault from './variants/MultiSelectDefault';
import MultiSelectLabeled from './variants/MultiSelectLabeled';
import SingleSelectDefault from './variants/SingleSelectDefault';
import SingleSelectLabeled from './variants/SingleSelectLabeled';
import SingleSelectCustom from './variants/SingleSelectCustom';
import MultiSelectCustom from './variants/MultiSelectCustom';

export default function SelectLabelRenderer<OptionType extends SelectOption>({
    variant,
    ...props
}: SelectLabelDisplayProps<OptionType>) {
    const { isMultiSelect, options, selectedValues } = props;

    const selectedOptions = useMemo(
        () => options.filter((opt) => selectedValues.includes(opt.value)),
        [options, selectedValues],
    );

    const getComponent = () => {
        if (isMultiSelect) {
            switch (variant) {
                case 'labeled':
                    return MultiSelectLabeled;
                case 'custom':
                    return MultiSelectCustom;
                default:
                    return MultiSelectDefault;
            }
        }

        switch (variant) {
            case 'labeled':
                return SingleSelectLabeled;
            case 'custom':
                return SingleSelectCustom;
            default:
                return SingleSelectDefault;
        }
    };

    return getComponent()({ ...props, selectedOptions });
}
