import { RenderOptions, render } from '@testing-library/react';
import React, { createRef, useEffect } from 'react';

// Same API and behavior as `renderHook` in @testing-library/react 13.1+, built on the v12 `render`
// we have today. Once React and @testing-library/react are upgraded, replace this file's body with
// `export { renderHook } from '@testing-library/react'`.

type RenderHookOptions<Props> = Omit<RenderOptions, 'queries'> & {
    initialProps?: Props;
};

type RenderHookResult<Result, Props> = {
    result: { current: Result };
    rerender: (props?: Props) => void;
    unmount: () => void;
};

export function renderHook<Result, Props = undefined>(
    renderCallback: (props: Props) => Result,
    options: RenderHookOptions<Props> = {},
): RenderHookResult<Result, Props> {
    const { initialProps, ...renderOptions } = options;
    const result = createRef<Result>() as { current: Result };

    function TestComponent({ hookProps }: { hookProps: Props }) {
        const pendingResult = renderCallback(hookProps);
        useEffect(() => {
            result.current = pendingResult;
        });
        return null;
    }

    const { rerender: baseRerender, unmount } = render(
        <TestComponent hookProps={initialProps as Props} />,
        renderOptions,
    );

    function rerender(props?: Props) {
        baseRerender(<TestComponent hookProps={props as Props} />);
    }

    return { result, rerender, unmount };
}
