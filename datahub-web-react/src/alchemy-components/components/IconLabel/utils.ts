// Helper function to validate image URLs
export const isValidImageUrl = async (url: string): Promise<boolean> => {
    return new Promise((resolve) => {
        const img = new Image();
        img.src = url;

        img.onload = () => resolve(true); // Image is valid
        img.onerror = () => resolve(false); // Image is invalid
    });
};
