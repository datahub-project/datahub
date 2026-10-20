import { render, screen } from '@testing-library/react';
import React from 'react';

import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';
import CustomThemeProvider from '@src/CustomThemeProvider';

function renderProfile(node: React.ReactNode) {
    return render(<CustomThemeProvider>{node}</CustomThemeProvider>);
}

describe('lazyProfileComponent', () => {
    it('shows a loader until the profile chunk resolves', async () => {
        let resolveChunk: (value: { default: React.FunctionComponent }) => void = () => undefined;
        const LazySection = lazyProfileComponent(
            'ExampleSection',
            () =>
                new Promise<{ default: React.FunctionComponent }>((resolve) => {
                    resolveChunk = resolve;
                }),
        );

        renderProfile(<LazySection />);
        expect(screen.getByLabelText('Loading...')).toBeInTheDocument();

        resolveChunk({ default: () => <div>Loaded profile section</div> });
        expect(await screen.findByText('Loaded profile section')).toBeInTheDocument();
        expect(screen.queryByLabelText('Loading...')).not.toBeInTheDocument();
    });

    it('renders the preloaded chunk', async () => {
        const LazySection = lazyProfileComponent('ExampleSection', () =>
            Promise.resolve({ default: () => <div>Loaded profile section</div> }),
        );

        await LazySection.preload();

        renderProfile(<LazySection />);
        expect(await screen.findByText('Loaded profile section')).toBeInTheDocument();
        expect(screen.queryByLabelText('Loading...')).not.toBeInTheDocument();
    });

    it('keeps the loaded section mounted when its parent re-renders', async () => {
        let mounts = 0;
        const LazySection = lazyProfileComponent('ExampleSection', () =>
            Promise.resolve({
                default: function Section() {
                    React.useEffect(() => {
                        mounts += 1;
                    }, []);
                    return <div>Loaded profile section</div>;
                },
            }),
        );

        const { rerender } = renderProfile(<LazySection />);
        expect(await screen.findByText('Loaded profile section')).toBeInTheDocument();
        const mountsAfterLoad = mounts;

        rerender(
            <CustomThemeProvider>
                <LazySection />
            </CustomThemeProvider>,
        );

        expect(screen.getByText('Loaded profile section')).toBeInTheDocument();
        expect(mounts).toBe(mountsAfterLoad);
    });

    it('asks for a page reload when the profile chunk fails', async () => {
        const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
        const LazySection = lazyProfileComponent('BrokenSection', () => Promise.reject(new Error('chunk failed')));

        renderProfile(<LazySection />);

        expect(await screen.findByRole('button', { name: 'Refresh' })).toBeInTheDocument();
        errorSpy.mockRestore();
    });

    it('renders the caller fallback with the component props while loading', () => {
        const pendingChunk = new Promise<{ default: React.FunctionComponent<{ title: string }> }>(() => {
            // Never resolves, so the fallback stays on screen.
        });
        const LazySection = lazyProfileComponent('ExampleSection', () => pendingChunk, {
            fallback: ({ title }) => <div>Loading {title}</div>,
        });

        renderProfile(<LazySection title="Lineage" />);
        expect(screen.getByText('Loading Lineage')).toBeInTheDocument();
    });

    describe('with propagateRenderErrors', () => {
        class ParentBoundary extends React.Component<{ children: React.ReactNode }, { failed: boolean }> {
            constructor(props: { children: React.ReactNode }) {
                super(props);
                this.state = { failed: false };
            }

            static getDerivedStateFromError() {
                return { failed: true };
            }

            render() {
                return this.state.failed ? <div>Parent retry</div> : this.props.children;
            }
        }

        it('hands render errors to the parent boundary', async () => {
            const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
            const LazySection = lazyProfileComponent(
                'BrokenRender',
                () =>
                    Promise.resolve({
                        default: () => {
                            throw new Error('render failed');
                        },
                    }),
                { propagateRenderErrors: true },
            );

            renderProfile(
                <ParentBoundary>
                    <LazySection />
                </ParentBoundary>,
            );

            expect(await screen.findByText('Parent retry')).toBeInTheDocument();
            expect(screen.queryByRole('button', { name: 'Refresh' })).not.toBeInTheDocument();
            errorSpy.mockRestore();
        });

        it('still asks for a page reload when the chunk fails', async () => {
            const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
            const LazySection = lazyProfileComponent('BrokenChunk', () => Promise.reject(new Error('chunk failed')), {
                propagateRenderErrors: true,
            });

            renderProfile(
                <ParentBoundary>
                    <LazySection />
                </ParentBoundary>,
            );

            expect(await screen.findByRole('button', { name: 'Refresh' })).toBeInTheDocument();
            expect(screen.queryByText('Parent retry')).not.toBeInTheDocument();
            errorSpy.mockRestore();
        });
    });
});
