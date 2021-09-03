export function capitalizeFirstLetter(str?: string | null) {
    if (!str) {
        return undefined;
    }
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
}
