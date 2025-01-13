// Helper function to validate image URLs
export const isValidImageUrl = (url: string): boolean => {
    return /\.(jpeg|jpg|gif|png|webp|svg)$/i.test(url);
};
