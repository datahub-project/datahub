import { Button } from '@src/alchemy-components';
import { GraphCard } from '@src/alchemy-components/components/GraphCard';
import React, { useState } from 'react';
import { ChangeHistoryDrawer } from './ChangeHistoryDrawer/ChangeHistoryDrawer';

type ChangeHistoryGraphProps = {
    urn?: string;
};

export default function ChangeHistoryGraph({ urn }: ChangeHistoryGraphProps) {
    const [showDrawer, setShowDrawer] = useState<boolean>(false);
    const [day, setDay] = useState<string | undefined>('2024-12-16');

    const onButtonClick = (selectedDay: string | undefined) => {
        setShowDrawer(true);
        setDay(selectedDay);
    };

    return (
        <>
            <GraphCard
                title="Change History"
                renderControls={() => (
                    <>
                        <Button onClick={() => onButtonClick('2024-12-16')}>2024-12-16</Button>
                        <Button onClick={() => onButtonClick('2024-12-15')}>2024-12-15</Button>
                        <Button onClick={() => onButtonClick('2024-12-14')}>2024-12-14</Button>
                        <Button onClick={() => onButtonClick('2024-12-13')}>2024-12-13</Button>
                        <Button onClick={() => onButtonClick(undefined)}>Last 100</Button>
                    </>
                )}
                // TODO: add rendering of Calendar Chart with Change history data
                renderGraph={() => null}
            />

            {urn && showDrawer && (
                <ChangeHistoryDrawer
                    selectedDay={day}
                    urn={urn}
                    open={showDrawer}
                    onClose={() => setShowDrawer(false)}
                />
            )}
        </>
    );
}
