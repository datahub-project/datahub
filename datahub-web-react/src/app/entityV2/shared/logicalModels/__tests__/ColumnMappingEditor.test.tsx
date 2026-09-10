import { render } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import ColumnMappingEditor from '@app/entityV2/shared/logicalModels/ColumnMappingEditor';
import themeV2 from '@conf/theme/themeV2';

describe('ColumnMappingEditor', () => {
    it('auto-prefills exact (case-insensitive) name matches', () => {
        const onChange = vi.fn();
        render(
            <ThemeProvider theme={themeV2}>
                <ColumnMappingEditor
                    parentColumns={['id', 'name']}
                    childColumns={['ID', 'other']}
                    value={[]}
                    onChange={onChange}
                />
            </ThemeProvider>,
        );
        expect(onChange).toHaveBeenCalled();
        const lastCall = onChange.mock.calls[onChange.mock.calls.length - 1][0];
        expect(lastCall).toEqual(expect.arrayContaining([{ parentFieldPath: 'id', childFieldPath: 'ID' }]));
    });
});
