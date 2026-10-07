import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { ApiSummaryTab } from '@app/entityV2/api/ApiSummaryTab';
import CustomThemeProvider from '@src/CustomThemeProvider';

const mockUseBaseEntity = vi.fn();
vi.mock('@app/entity/shared/EntityContext', () => ({
    useBaseEntity: () => mockUseBaseEntity(),
}));
vi.mock('react-i18next', () => ({
    useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock('@app/entityV2/api/SignatureTab', () => ({ default: () => <div data-testid="signature" /> }));
vi.mock('@app/entityV2/shared/summary/SummaryAboutSection', () => ({ default: () => <div data-testid="about" /> }));

function renderTab(entity: unknown) {
    mockUseBaseEntity.mockReturnValue({ entity });
    return render(
        <CustomThemeProvider>
            <ApiSummaryTab />
        </CustomThemeProvider>,
    );
}

describe('ApiSummaryTab', () => {
    it('renders facts, the signature, and the reference link', () => {
        renderTab({
            __typename: 'Api',
            subTypes: { typeNames: ['MCP_TOOL'] },
            dataPlatformInstance: { platform: { name: 'langchain' } },
            properties: { externalUrl: 'https://docs.example.com/tool' },
        });

        expect(screen.getByText('MCP_TOOL')).toBeInTheDocument();
        expect(screen.getByText('langchain')).toBeInTheDocument();
        // Signature is always promoted into the landing tab.
        expect(screen.getByTestId('signature')).toBeInTheDocument();
        // Reference link points at the external docs url.
        const ref = screen.getByText('api.summary.viewDocs').closest('a');
        expect(ref).toHaveAttribute('href', 'https://docs.example.com/tool');
    });

    it('omits the reference section when there is no external url', () => {
        renderTab({ __typename: 'Api', subTypes: { typeNames: ['FUNCTION'] }, properties: {} });

        expect(screen.queryByText('api.summary.viewDocs')).not.toBeInTheDocument();
        expect(screen.getByTestId('signature')).toBeInTheDocument();
    });

    it('renders HTTP method and path for a REST endpoint', () => {
        renderTab({
            __typename: 'Api',
            subTypes: { typeNames: ['REST_ENDPOINT'] },
            restProperties: { method: 'POST', path: '/orders' },
            properties: {},
        });

        expect(screen.getByText('REST_ENDPOINT')).toBeInTheDocument();
        expect(screen.getByText('POST')).toBeInTheDocument();
        expect(screen.getByText('/orders')).toBeInTheDocument();
    });
});
