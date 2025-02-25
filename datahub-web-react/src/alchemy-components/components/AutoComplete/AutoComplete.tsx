import { AutoComplete as AntdAutoComplete } from 'antd';
import React from 'react';
import { colors, spacing } from '@src/alchemy-components/theme';
import radius from '@src/alchemy-components/theme/foundations/radius';
import { BOX_SHADOW, ChildrenWrapper, DropdownWrapper } from './components';
import { AutoCompleteProps } from './types';

// Additional offset to handle wrapper's padding (when `showWrapping` is enabled)
const DROPDOWN_ALIGN_WITH_WRAPPING = { offset: [0, -8] };

export default function AutoComplete({
    children,
    showWrapping,
    dropdownContentHeight,
    dataTestId,
    ...props
}: React.PropsWithChildren<AutoCompleteProps>) {
    const { open } = props;

    return (
        <div>
            <AntdAutoComplete
                {...props}
                listHeight={dropdownContentHeight}
                data-testid={dataTestId}
                dropdownRender={(menu) => {
                    return <DropdownWrapper>{props?.dropdownRender?.(menu) ?? menu}</DropdownWrapper>;
                }}
                dropdownAlign={{ ...(showWrapping ? DROPDOWN_ALIGN_WITH_WRAPPING : {}) }}
                dropdownStyle={{
                    ...(showWrapping
                        ? {
                              padding: spacing.xsm,
                              borderRadius: `${radius.none} ${radius.none} ${radius.lg} ${radius.lg}`,
                              backgroundColor: colors.gray[1500],
                              boxShadow: BOX_SHADOW,
                          }
                        : { borderRadius: radius.lg }),
                    ...(props?.dropdownStyle ?? {}),
                }}
            >
                <ChildrenWrapper $open={open} $showWrapping={showWrapping}>
                    {children}
                </ChildrenWrapper>
            </AntdAutoComplete>
        </div>
    );
}
