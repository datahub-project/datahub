import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import EntityFormModal from '@app/entityV2/shared/entityForm/EntityFormModal';
import { colors } from '@src/alchemy-components';

function hexToRgb(hex: string): string {
    const [, r, g, b] = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(hex) ?? [];
    return `rgb(${parseInt(r, 16)}, ${parseInt(g, 16)}, ${parseInt(b, 16)})`;
}

vi.mock('@src/app/entity/shared/entityForm/EntityFormContextProvider', () => ({
    default: ({ children }: { children: React.ReactNode }) => <div data-testid="mocked-form-context">{children}</div>,
}));

vi.mock('@src/app/entity/shared/entityForm/FormHeader/FormPageHeader', () => ({
    default: () => <div data-testid="mocked-form-page-header">Mocked FormPageHeader</div>,
}));

vi.mock('@app/entityV2/shared/entityForm/EntityForm', () => ({
    default: () => <div data-testid="mocked-entity-form">Mocked EntityForm</div>,
}));

describe('EntityFormModal', () => {
    it('renders a visible close icon with a color that contrasts against its white background', () => {
        render(<EntityFormModal selectedFormUrn="urn:li:form:test" isFormVisible hideFormModal={vi.fn()} />);

        // antd's Modal renders closeIcon inside a button with aria-label "Close"
        const closeButton = screen.getByLabelText('Close');
        const icon = closeButton.querySelector('svg') ?? closeButton;

        // Regression guard for the invisible-icon bug: previously this resolved to
        // theme.colors.textOnFillDefault (near white), invisible against this modal's plain
        // white header. Must never be a white/near-white value again.
        const renderedColor = window.getComputedStyle(icon).color;
        expect(renderedColor).not.toBe('white');
        expect(renderedColor).not.toBe('rgb(255, 255, 255)');
        // jsdom normalizes styled-components' hex value to rgb() on read-back.
        expect(renderedColor).toBe(hexToRgb(colors.gray[600]));
    });

    it('calls hideFormModal when the close icon is clicked', () => {
        const mockHideFormModal = vi.fn();
        render(<EntityFormModal selectedFormUrn="urn:li:form:test" isFormVisible hideFormModal={mockHideFormModal} />);

        fireEvent.click(screen.getByLabelText('Close'));

        expect(mockHideFormModal).toHaveBeenCalledTimes(1);
    });

    it('renders nothing visible when isFormVisible is false', () => {
        render(<EntityFormModal selectedFormUrn="urn:li:form:test" isFormVisible={false} hideFormModal={vi.fn()} />);

        expect(screen.queryByTestId('mocked-form-page-header')).not.toBeInTheDocument();
    });
});
