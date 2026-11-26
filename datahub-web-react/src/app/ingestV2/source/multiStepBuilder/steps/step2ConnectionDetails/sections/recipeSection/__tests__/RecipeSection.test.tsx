import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { RecipeSection } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/RecipeSection';
import { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';

vi.mock('@app/ingestV2/shared/hooks/useCapabilitySummary', () => ({
    useCapabilitySummary: () => ({ getConnectorsWithTestConnection: () => new Set(['teradata']) }),
}));
vi.mock('@app/ingestV2/source/builder/RecipeForm/TestConnection/TestConnectionButton', () => ({
    default: () => <button type="button">Test Connection</button>,
}));
vi.mock('@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/YamlEditor', () => ({
    YamlEditor: () => <div>yaml editor</div>,
}));
vi.mock('@app/sharedV2/forms/multiStepForm/MultiStepFormContext', () => ({
    useMultiStepContext: () => ({ state: {} }),
}));

function renderSection(type: string) {
    render(
        <RecipeSection
            state={{ type } as MultiStepSourceBuilderState}
            displayRecipe={`source:\n    type: ${type}`}
            setStagedRecipe={vi.fn()}
        />,
    );
}

describe('RecipeSection', () => {
    // Sources without a structured form only get the YAML editor; Test Connection must not depend on the form.
    it('offers Test Connection next to the YAML editor when the source supports it', () => {
        renderSection('teradata');

        expect(screen.getByText('yaml editor')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Test Connection' })).toBeInTheDocument();
    });

    it('shows only the YAML editor when the source does not support Test Connection', () => {
        renderSection('excel');

        expect(screen.getByText('yaml editor')).toBeInTheDocument();
        expect(screen.queryByRole('button', { name: 'Test Connection' })).not.toBeInTheDocument();
    });
});
