import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { LoadedVideo } from '@components/components/LoadedVideo/LoadedVideo';

import themeV2 from '@conf/theme/themeV2';

function renderVideo(props: Partial<React.ComponentProps<typeof LoadedVideo>> = {}) {
    return render(
        <ThemeProvider theme={themeV2}>
            <LoadedVideo mp4Src="/missing.mp4" {...props} />
        </ThemeProvider>,
    );
}

describe('LoadedVideo', () => {
    it('shows the error state when the only source fails to load', () => {
        const onVideoError = vi.fn();
        const { container } = renderVideo({ onVideoError });

        const source = container.querySelector('source[type="video/mp4"]');
        expect(source).not.toBeNull();
        fireEvent.error(source as Element);

        expect(onVideoError).toHaveBeenCalledTimes(1);
        expect(container.querySelector('video')).toBeNull();
        expect(screen.getByText('Failed to load video')).toBeInTheDocument();
    });

    it('waits for the last source before showing the error state', () => {
        const onVideoError = vi.fn();
        const { container } = renderVideo({ webmSrc: '/missing.webm', onVideoError });

        fireEvent.error(container.querySelector('source[type="video/mp4"]') as Element);
        expect(onVideoError).not.toHaveBeenCalled();
        expect(container.querySelector('video')).not.toBeNull();

        fireEvent.error(container.querySelector('source[type="video/webm"]') as Element);
        expect(onVideoError).toHaveBeenCalledTimes(1);
        expect(container.querySelector('video')).toBeNull();
    });
});
