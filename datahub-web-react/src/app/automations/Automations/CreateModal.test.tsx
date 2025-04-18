import { render } from '@testing-library/react';
import React from 'react';

import { AutomationCreateModal } from '@app/automations/Automations/CreateModal';
import * as SnowflakeTagPropagation from '@app/automations/recipes/snowflake/tagPropagation';
import { AppConfigContext, DEFAULT_APP_CONFIG } from '@src/appConfigContext';

describe('CreateModal', () => {
    it('should display Snowflake automation when enabled', () => {
        const mockAppConfig = {
            ...DEFAULT_APP_CONFIG,
            classificationConfig: {
                ...DEFAULT_APP_CONFIG.classificationConfig,
                automations: {
                    ...DEFAULT_APP_CONFIG.classificationConfig.automations,
                    snowflake: true,
                },
            },
        };
        const mockAppConfigContext = {
            config: mockAppConfig,
            loaded: false,
            refreshContext: () => null,
        };
        const { queryByText } = render(
            <AppConfigContext.Provider value={mockAppConfigContext}>
                <AutomationCreateModal isOpen setIsOpen={() => null} />
            </AppConfigContext.Provider>,
        );
        expect(queryByText(SnowflakeTagPropagation.template.name)).toBeInTheDocument();
    });

    it('should not display Snowflake automation when disabled', () => {
        const mockAppConfig = {
            ...DEFAULT_APP_CONFIG,
            classificationConfig: {
                ...DEFAULT_APP_CONFIG.classificationConfig,
                automations: {
                    ...DEFAULT_APP_CONFIG.classificationConfig.automations,
                    snowflake: false,
                },
            },
        };
        const mockAppConfigContext = {
            config: mockAppConfig,
            loaded: false,
            refreshContext: () => null,
        };
        const { queryByText } = render(
            <AppConfigContext.Provider value={mockAppConfigContext}>
                <AutomationCreateModal isOpen setIsOpen={() => null} />
            </AppConfigContext.Provider>,
        );
        expect(queryByText(SnowflakeTagPropagation.template.name)).not.toBeInTheDocument();
    });
});
