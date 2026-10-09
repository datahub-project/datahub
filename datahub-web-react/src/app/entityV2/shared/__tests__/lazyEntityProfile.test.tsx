import { render, screen } from '@testing-library/react';
import React from 'react';

import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import CustomThemeProvider from '@src/CustomThemeProvider';

function renderProfile(node: React.ReactNode) {
    return render(<CustomThemeProvider>{node}</CustomThemeProvider>);
}

describe('lazyProfileComponent', () => {
    it('shows a loader until the profile chunk resolves', async () => {
        let resolveChunk: (value: { default: React.ComponentType }) => void = () => undefined;
        const LazySection = lazyProfileComponent(
            'ExampleSection',
            () =>
                new Promise((resolve) => {
                    resolveChunk = resolve;
                }),
        );

        renderProfile(<LazySection />);
        expect(screen.getByLabelText('Loading...')).toBeInTheDocument();

        resolveChunk({ default: () => <div>Loaded profile section</div> });
        expect(await screen.findByText('Loaded profile section')).toBeInTheDocument();
        expect(screen.queryByLabelText('Loading...')).not.toBeInTheDocument();
    });

    it('asks for a page reload when the profile chunk fails', async () => {
        const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
        const LazySection = lazyProfileComponent('BrokenSection', () => Promise.reject(new Error('chunk failed')));

        renderProfile(<LazySection />);

        expect(await screen.findByRole('button', { name: 'Refresh' })).toBeInTheDocument();
        errorSpy.mockRestore();
    });
});
