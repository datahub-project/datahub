import React, { useState } from 'react';

import { AutomationTypes } from '@app/automations/constants';
import { getTemplate, parseJSON } from '@app/automations/utils';

import { AutomationContextProvider } from '../AutomationProvider';
import { ListCard } from '../components';
import { AutomationEditModal } from '../EditModal';

import { ActionCard } from './ActionCard';
import { TestCard } from './TestCard';

export const AutomationsListCard = ({ automation }: any) => {
    const { type, urn, category, name, description } = automation;
    const [isOpen, setIsOpen] = useState(false);

    const definition = parseJSON(automation?.definition);

    return (
        <AutomationContextProvider
            context={{
                urn: urn || automation.key,
                type,
                name,
                category,
                description,
                definition,
                localTemplate: getTemplate(definition.action?.type) || ({} as any),
            }}
        >
            <ListCard>
                {type === AutomationTypes.ACTION && (
                    <ActionCard automation={automation} openEditModal={() => setIsOpen(true)} />
                )}
                {type === AutomationTypes.TEST && (
                    <TestCard automation={automation} openEditModal={() => setIsOpen(true)} />
                )}
            </ListCard>
            <AutomationEditModal isOpen={isOpen} setIsOpen={setIsOpen} />
        </AutomationContextProvider>
    );
};
