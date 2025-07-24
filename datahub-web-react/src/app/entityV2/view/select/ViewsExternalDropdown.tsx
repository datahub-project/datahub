import { Dropdown, zIndices } from '@components';
import React, { useEffect } from 'react';

import { ViewBuilder } from '@app/entityV2/view/builder/ViewBuilder';
import { useViewsSelectContext } from '@app/entityV2/view/select/ViewSelectContext';
import ViewsExternalDropdownContent from '@app/entityV2/view/select/ViewsExternalDropdownContent';

interface Props {
    disabled?: boolean;
    className?: string;
}

export default function ViewsExternalDropdown({ disabled, className, children }: React.PropsWithChildren<Props>) {
    const { isInternalOpen, updateOpenState, viewBuilderDisplayState, onCloseViewBuilder } = useViewsSelectContext();

    // Automatically close the dropdown on resize to avoid the dropdown's misalignment
    useEffect(() => {
        const onResize = () => updateOpenState(false);
        window.addEventListener('resize', onResize, true);
        return () => window.removeEventListener('resize', onResize);
    }, [updateOpenState]);

    if (disabled) return <>{children}</>;

    return (
        <>
            <Dropdown
                open={isInternalOpen}
                dropdownRender={() => <ViewsExternalDropdownContent className={className} />}
                onOpenChange={updateOpenState}
                overlayStyle={{ zIndex: zIndices.dropdown }}
                placement="bottomLeft"
            >
                {children}
            </Dropdown>
            {viewBuilderDisplayState.visible && (
                <ViewBuilder
                    urn={viewBuilderDisplayState.view?.urn || undefined}
                    initialState={viewBuilderDisplayState.view}
                    mode={viewBuilderDisplayState.mode}
                    onSubmit={onCloseViewBuilder}
                    onCancel={onCloseViewBuilder}
                />
            )}
        </>
    );
}
