import { render, screen } from '@testing-library/react';
import React, { Suspense } from 'react';
import { describe, expect, it } from 'vitest';

import { lazyDefault, lazyNamed } from '@app/shared/lazyPage';

function Page({ label }: { label: string }) {
    return <div>{label}</div>;
}

describe('lazy page loaders', () => {
    it('renders the default export of a dynamic import', async () => {
        const LazyPage = lazyDefault(async () => ({ default: Page }));
        render(
            <Suspense fallback={null}>
                <LazyPage label="default page" />
            </Suspense>,
        );
        expect(await screen.findByText('default page')).toBeInTheDocument();
    });

    it('renders a named export of a dynamic import', async () => {
        const LazyPage = lazyNamed(async () => ({ Page }), 'Page');
        render(
            <Suspense fallback={null}>
                <LazyPage label="named page" />
            </Suspense>,
        );
        expect(await screen.findByText('named page')).toBeInTheDocument();
    });
});
