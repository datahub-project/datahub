import React, { useState } from 'react';

import { parseJSON } from '@app/automations/utils';

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
            key={urn}
            context={{
                urn,
                type,
                initialName: name,
                initialCategory: category,
                initialDescription: description,
                initialExecutorId: config?.executorId,
                initialRecipe: parseJSON(config?.recipe),
            }}
        >
            <ListCard>
                <ActionCard automation={automation} openEditModal={() => setIsOpen(true)} />
            </ListCard>
            <AutomationEditModal isOpen={isOpen} setIsOpen={setIsOpen} />
        </AutomationContextProvider>
    );
};
