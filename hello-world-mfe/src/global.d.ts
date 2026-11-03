// TypeScript declarations for Webpack
declare const module: {
    hot?: {
        accept(path: string, callback: () => void): void;
    };
};

