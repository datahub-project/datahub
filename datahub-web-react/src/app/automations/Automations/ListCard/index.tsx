import React, { useState } from 'react';

import { getTemplate, parseJSON } from '@app/automations/utils';

import { AutomationContextProvider } from '../AutomationProvider';
import { ListCard } from '../components';
import { AutomationEditModal } from '../EditModal';

import { ActionCard } from './ActionCard';

export const AutomationsListCard = ({ automation }: any) => {
    const { urn, details } = automation;
    const { type, name, category, description, config } = details;
    const [isOpen, setIsOpen] = useState(false);

    return (
        <AutomationContextProvider
            context={{
                urn,
                type,
                name,
                category,
                description,
                definition: parseJSON(config?.recipe),
                localTemplate: getTemplate(type) || ({} as any),
            }}
        >
            <ListCard>
                <ActionCard automation={automation} openEditModal={() => setIsOpen(true)} />
            </ListCard>
            <AutomationEditModal isOpen={isOpen} setIsOpen={setIsOpen} />
        </AutomationContextProvider>
    );
};
