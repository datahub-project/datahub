import React from 'react';
import styled from 'styled-components';

import { Loader } from '@src/alchemy-components/components/Loader/Loader';
import { NestedSelect, SelectProps } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';

const ScrollTrigger = styled.div`
    height: 1px;
    width: 100%;
    opacity: 0;
    pointer-events: none;
`;

const LoadingContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 12px;
    border-top: 1px solid #f0f0f0;
    background: white;
`;

export interface InfiniteScrollNestedSelectProps<OptionType extends NestedSelectOption = NestedSelectOption>
    extends Omit<SelectProps<OptionType>, 'options'> {
    options: OptionType[];
    loading?: boolean;
    hasMore?: boolean;
    scrollRef?: ((node?: Element | null) => void) | React.RefObject<HTMLDivElement>;
}

/**
 * Enhanced NestedSelect with infinite scroll capability
 * Adds a scroll trigger element and loading indicator for infinite scroll functionality
 */
export function InfiniteScrollNestedSelect<OptionType extends NestedSelectOption = NestedSelectOption>({
    options,
    loading = false,
    hasMore = false,
    scrollRef,
    ...selectProps
}: InfiniteScrollNestedSelectProps<OptionType>) {
    // Render the scroll trigger and loading indicator as additional options
    const enhancedOptions = React.useMemo(() => {
        const baseOptions = [...options];

        // Add scroll trigger as the last option if we have more data to load
        if (hasMore && !loading) {
            baseOptions.push({
                value: '__scroll_trigger__',
                label: '',
                id: '__scroll_trigger__',
                isScrollTrigger: true,
            } as unknown as OptionType);
        }

        // Add loading indicator if currently loading
        if (loading && options.length > 0) {
            baseOptions.push({
                value: '__loading__',
                label: '',
                id: '__loading__',
                isLoadingIndicator: true,
            } as unknown as OptionType);
        }

        return baseOptions;
    }, [options, hasMore, loading]);

    const renderCustomOptionText = React.useCallback(
        (option: OptionType) => {
            // Handle scroll trigger
            if ((option as any).isScrollTrigger) {
                // Handle both callback ref and RefObject
                const refProp = typeof scrollRef === 'function' ? { ref: scrollRef } : { ref: scrollRef };
                return <ScrollTrigger {...refProp} />;
            }

            // Handle loading indicator
            if ((option as any).isLoadingIndicator) {
                return (
                    <LoadingContainer>
                        <Loader size="sm" />
                    </LoadingContainer>
                );
            }

            // Use parent's custom renderer if provided
            if (selectProps.renderCustomOptionText) {
                return selectProps.renderCustomOptionText(option);
            }

            // Default rendering
            return option.label;
        },
        [scrollRef, selectProps],
    );

    return <NestedSelect {...selectProps} options={enhancedOptions} renderCustomOptionText={renderCustomOptionText} />;
}
