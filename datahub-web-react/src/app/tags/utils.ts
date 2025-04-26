import ColorHash from 'color-hash';

// Color hash generator - consistent colors for same tag
const generateColor = new ColorHash({
    saturation: 0.9,
});

export const getTagColor = (entity: any): string => {
    try {
        // Check direct properties first
        if (entity.properties?.colorHex) {
            return entity.properties.colorHex;
        }

        // Check for tagProperties aspect
        if (entity.aspects && Array.isArray(entity.aspects)) {
            const tagProps = entity.aspects.find((a: any) => a.name === 'tagProperties');
            if (tagProps?.data?.colorHex) {
                return tagProps.data.colorHex;
            }
        }

        // Check for aspects.tagProperties path
        if (entity.aspects?.tagProperties?.colorHex) {
            return entity.aspects.tagProperties.colorHex;
        }

        // If no color is found, generate one from the URN
        if (entity.urn) {
            return generateColor.hex(entity.urn);
        }
    } catch (e) {
        console.error('Error accessing tag color', e);
    }

    // Default color if all else fails
    return '#BFBFBF';
};
