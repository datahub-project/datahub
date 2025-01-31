import { useMemo } from 'react';
import { SelectLabelDisplayProps } from '../../types';
import MultiSelectDefault from './variants/MultiSelectDefault';
import MultiSelectLabeled from './variants/MultiSelectLabeled';
import SingleSelectDefault from './variants/SingleSelectDefault';
import SingleSelectLabeled from './variants/SingleSelectLabeled';

export default function SelectLabelRenderer({ variant, ...props }: SelectLabelDisplayProps) {
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
                default:
                    return MultiSelectDefault;
            }
        }

        switch (variant) {
            case 'labeled':
                return SingleSelectLabeled;
            default:
                return SingleSelectDefault;
        }
    };

    return getComponent()({ ...props, selectedOptions });
}
