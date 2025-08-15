import React, { useContext } from 'react';

export interface HoverEntityTooltipContextType {
    entityCount: number | undefined;
}

export const HoverEntityTooltipContext = React.createContext<HoverEntityTooltipContextType>({
    entityCount: undefined,
});

export const useHoverEntityTooltipContext = () => {
    const { entityCount } = useContext(HoverEntityTooltipContext);
    return { entityCount };
};
