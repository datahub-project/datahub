import { AutoComplete as AntdAutoComplete } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import ClickOutside from '../Utils/ClickOutside/ClickOutside';
import { DropdownWrapper } from './components';
import { AUTOCOMPLETE_WRAPPER_CLASS_CSS_SELECTOR, AUTOCOMPLETE_WRAPPER_CLASS_NAME, ESCAPE_KEY } from './constants';
import { AutoCompleteProps, OptionType } from './types';
import { OverlayClassProvider } from '../Utils';

export default function AutoComplete({
    children,
    dropdownContentHeight,
    dataTestId,
    onDropdownVisibleChange,
    onChange,
    onClear,
    value,
    ...props
}: React.PropsWithChildren<AutoCompleteProps>) {
    const [internalValue, setInternalValue] = useState<string>(value || '');
    const [internalOpen, setInternalOpen] = useState<boolean>(true);

    useEffect(() => {
        onDropdownVisibleChange?.(internalOpen);
    }, [internalOpen, onDropdownVisibleChange]);

    const onChangeHandler = (newValue: string, option: OptionType | OptionType[]) => {
        setInternalValue(newValue);
        if (!internalOpen && newValue !== '') setInternalOpen(true);
        onChange?.(newValue, option);
    };

    const onKeyDownHandler = useCallback(
        (event: React.KeyboardEvent) => {
            if (event.key === ESCAPE_KEY) {
                if (internalOpen) {
                    setInternalOpen(false);
                } else {
                    setInternalValue('');
                    onClear?.();
                }
            }
        },
        [internalOpen, setInternalValue, onClear],
    );

    const onBlur = (event: React.FocusEvent) => {
        if (
            event.relatedTarget &&
            !(event.relatedTarget as HTMLElement).closest(AUTOCOMPLETE_WRAPPER_CLASS_CSS_SELECTOR)
        ) {
            setInternalOpen(false);
        }
    };

    return (
        <ClickOutside
            ignoreSelector={AUTOCOMPLETE_WRAPPER_CLASS_CSS_SELECTOR}
            onClickOutside={() => setInternalOpen(false)}
        >
            <AntdAutoComplete
                open={internalOpen}
                value={internalValue}
                {...props}
                listHeight={dropdownContentHeight}
                data-testid={dataTestId}
                onClick={() => setInternalOpen(true)}
                onBlur={onBlur}
                onClear={onClear}
                onKeyDown={onKeyDownHandler}
                onChange={onChangeHandler}
                dropdownRender={(menu) => {
                    return (
                        // Pass overlay class name to children to add possibility not to close autocomplete by clicking on child's portals
                        // as ClickOutside will ignore them
                        <OverlayClassProvider overlayClassName={AUTOCOMPLETE_WRAPPER_CLASS_NAME}>
                            <DropdownWrapper className={AUTOCOMPLETE_WRAPPER_CLASS_NAME}>
                                {props?.dropdownRender?.(menu) ?? menu}
                            </DropdownWrapper>
                        </OverlayClassProvider>
                    );
                }}
            >
                {children}
            </AntdAutoComplete>
        </ClickOutside>
    );
}
