import React, { useState } from 'react';

import { AutomationContextProvider } from '@app/automations/Automations/AutomationProvider';
import { AutomationEditModal } from '@app/automations/Automations/EditModal';
import { ActionCard } from '@app/automations/Automations/ListCard/ActionCard';
import { ListCard } from '@app/automations/Automations/components';
import { parseJSON } from '@app/automations/utils';

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
