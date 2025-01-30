const colors = ['#3a9c7b', '#487a56', '#3a829c', '#3a4c9c', '#783a9c', '#d15858'];

export const getFilterColor = (field: string, value: string) => {
    const typeStr = field + value;

    let hash = 0;
    for (let i = 0; i < typeStr.length; i++) {
        /* eslint-disable no-bitwise */
        hash = (hash << 5) - hash + typeStr.charCodeAt(i);
        /* eslint-disable no-bitwise */
        hash |= 0; // Convert to 32bit integer
    }

    const colorIndex = Math.abs(hash) % colors.length;
    return colors[colorIndex];
};
