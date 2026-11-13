import { render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import ContainerSummaryTab from '@app/entityV2/container/ContainerSummaryTab';
import { useEntityData } from '@app/entityV2/shared/EntityContext';

vi.mock('@app/entityV2/shared/EntityContext', () => ({ useEntityData: vi.fn() }));
vi.mock('@app/entityV2/container/tableau/TableauWorkbookSummaryTab', () => ({
    default: () => <div>tableau summary</div>,
}));
vi.mock('@app/entityV2/container/workbook/WorkbookSummaryTab', () => ({
    default: () => <div>workbook summary</div>,
}));

function renderWithSubTypes(typeNames: string[]) {
    vi.mocked(useEntityData).mockReturnValue({ entityData: { subTypes: { typeNames } } } as any);
    return render(<ContainerSummaryTab />);
}

describe('ContainerSummaryTab', () => {
    it('shows the Tableau summary for a Tableau workbook', () => {
        renderWithSubTypes(['Workbook']);
        expect(screen.getByText('tableau summary')).toBeInTheDocument();
    });

    it('shows the generic workbook summary for another platform whose own subtype comes first', () => {
        renderWithSubTypes(['Qlik App', 'Workbook']);
        expect(screen.getByText('workbook summary')).toBeInTheDocument();
    });

    it('shows nothing for other containers', () => {
        const { container } = renderWithSubTypes(['Qlik Space']);
        expect(container).toBeEmptyDOMElement();
    });
});
