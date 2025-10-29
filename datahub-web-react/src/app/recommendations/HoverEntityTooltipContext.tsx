import React, { useContext } from 'react';

interface HoverEntityTooltipContextType {
    entityCount: number | undefined;
}

export const HoverEntityTooltipContext = React.createContext<HoverEntityTooltipContextType>({
    entityCount: undefined,
});

const useHoverEntityTooltipContext = () => {
    const { entityCount } = useContext(HoverEntityTooltipContext);
    return { entityCount };
};
