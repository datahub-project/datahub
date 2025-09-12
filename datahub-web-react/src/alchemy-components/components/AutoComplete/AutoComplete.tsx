import { AutoComplete as AntdAutoComplete } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';

import { DropdownWrapper } from '@components/components/AutoComplete/components';
import {
    AUTOCOMPLETE_WRAPPER_CLASS_CSS_SELECTOR,
    AUTOCOMPLETE_WRAPPER_CLASS_NAME,
    ESCAPE_KEY,
} from '@components/components/AutoComplete/constants';
import { AutoCompleteProps, OptionType } from '@components/components/AutoComplete/types';
import { OverlayClassProvider } from '@components/components/Utils';
import ClickOutside from '@components/components/Utils/ClickOutside/ClickOutside';

export default function AutoComplete({
    children,
    dropdownContentHeight,
    dataTestId,
    onDropdownVisibleChange,
    onChange,
    onClear,
    value,
    clickOutsideWidth,
    ...props
}: React.PropsWithChildren<AutoCompleteProps>) {
    const { open } = props;

    const [internalOpen, setInternalOpen] = useState<boolean>(!!open);

    useEffect(() => {
        onDropdownVisibleChange?.(internalOpen);
    }, [internalOpen, onDropdownVisibleChange]);

    useEffect(() => {
        if (open !== undefined) setInternalOpen(open);
    }, [open]);

    const onChangeHandler = (newValue: string, option: OptionType | OptionType[]) => {
        if (!internalOpen && newValue !== '') setInternalOpen(true);
        onChange?.(newValue, option);
    };

    const onKeyDownHandler = useCallback(
        (event: React.KeyboardEvent) => {
            if (event.key === ESCAPE_KEY) {
                if (internalOpen) {
                    setInternalOpen(false);
                } else {
                    onClear?.();
                }
            }
        },
        [internalOpen, onClear],
    );

    const onBlur = (event: React.FocusEvent) => {
        if (
            event.relatedTarget &&
            !(event.relatedTarget as HTMLElement).closest(AUTOCOMPLETE_WRAPPER_CLASS_CSS_SELECTOR)
        ) {
            setInternalOpen(false);
        }
    };

    // Automatically close the dropdown on resize to avoid the dropdown's misalignment
    useEffect(() => {
        const onResize = () => setInternalOpen(false);
        window.addEventListener('resize', onResize, true);
        return () => window.removeEventListener('resize', onResize);
    }, []);

    return (
        <ClickOutside
            ignoreSelector={AUTOCOMPLETE_WRAPPER_CLASS_CSS_SELECTOR}
            onClickOutside={() => setInternalOpen(false)}
            width={clickOutsideWidth}
        >
            <AntdAutoComplete
                open={internalOpen}
                value={value}
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
