import React from 'react';
import styled from 'styled-components';

import { Loader } from '@components/components/Loader/Loader';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';
import { SelectOption, SelectProps } from '@components/components/Select/types';

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
    background: ${(props) => props.theme.colors.bg};
`;

interface InfiniteScrollSimpleSelectProps<OptionType extends SelectOption = SelectOption>
    extends Omit<SelectProps<OptionType>, 'options'> {
    options: OptionType[];
    loading?: boolean;
    hasMore?: boolean;
    scrollRef?: ((node?: Element | null) => void) | React.RefObject<HTMLDivElement>;
}

/**
 * Enhanced SimpleSelect with infinite scroll capability
 * Adds a scroll trigger element and loading indicator for infinite scroll functionality
 *
 */
export function InfiniteScrollSimpleSelect<OptionType extends SelectOption = SelectOption>({
    options,
    loading = false,
    hasMore = false,
    scrollRef,
    renderCustomOptionText,
    ...selectProps
}: InfiniteScrollSimpleSelectProps<OptionType>) {
    const enhancedOptions = React.useMemo(() => {
        const baseOptions = [...options];

        if (hasMore && !loading) {
            baseOptions.push({
                value: '__scroll_trigger__',
                label: '__scroll_trigger__',
                isScrollTrigger: true,
                isDisabled: true,
            } as unknown as OptionType);
        }

        if (loading && options.length > 0) {
            baseOptions.push({
                value: '__loading__',
                label: '__loading__',
                isLoadingIndicator: true,
                isDisabled: true,
            } as unknown as OptionType);
        }

        return baseOptions;
    }, [options, hasMore, loading]);

    const handleRenderCustomOptionText = React.useCallback(
        (option: OptionType) => {
            if ((option as any).isScrollTrigger) {
                const refProp = typeof scrollRef === 'function' ? { ref: scrollRef } : { ref: scrollRef };
                return <ScrollTrigger {...refProp} />;
            }

            if ((option as any).isLoadingIndicator) {
                return (
                    <LoadingContainer>
                        <Loader size="sm" />
                    </LoadingContainer>
                );
            }

            if (renderCustomOptionText) {
                return renderCustomOptionText(option);
            }

            return option.label || '';
        },
        [scrollRef, renderCustomOptionText],
    );

    return (
        <SimpleSelect
            {...selectProps}
            options={enhancedOptions}
            renderCustomOptionText={handleRenderCustomOptionText}
        />
    );
}
