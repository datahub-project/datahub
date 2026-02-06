# Building Custom Micro-Frontends for DataHub

This guide explains how DataHub users can build, deploy, and integrate their own micro-frontend (MFE) applications into DataHub.

## Overview

DataHub supports hosting custom micro-frontends using [Webpack Module Federation](https://webpack.js.org/concepts/module-federation/). This allows you to:

- Build custom UI extensions using **any frontend framework** (React, Vue, Angular, Svelte, etc.)
- Host your MFE on your own infrastructure
- Seamlessly integrate with the DataHub navigation and UI

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DataHub (Host)                        │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────────┐   │
│  │  DataHub UI   │  │  MFE Loader   │  │  Navigation       │   │
│  │  (React)      │  │  (Federation) │  │  (Shows your MFE) │   │
│  └───────────────┘  └───────┬───────┘  └───────────────────┘   │
│                             │                                   │
│                             │ Loads remoteEntry.js              │
│                             ▼                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ HTTPS
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                Your Infrastructure (Remote)                     │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  Your MFE App                                              │ │
│  │  - remoteEntry.js (Webpack Module Federation)             │ │
│  │  - mount() function                                        │ │
│  │  - Your custom UI components                               │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  Hosted on: AWS S3, CloudFront, Vercel, Netlify, your servers  │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Step 1: Use the MFE Starter Template

Copy the `hello-world-mfe` directory as your starting point:

```bash
# Clone or copy the template
cp -r hello-world-mfe my-custom-mfe
cd my-custom-mfe

# Update package.json with your app name
# Edit webpack.config.js to change the federation name
# Customize src/App.tsx with your UI
```

### Step 2: Understand the Mount Function Contract

**This is the most important requirement.** Your MFE must export a `mount` function:

```typescript
// src/mount.tsx
export function mount(
    container: HTMLElement, 
    options: Record<string, unknown>
): () => void {
    // Render your app into the container
    // ...
    
    // Return a cleanup function
    return () => {
        // Cleanup/unmount your app
    };
}
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `container` | `HTMLElement` | The DOM element where your app should render |
| `options` | `object` | Options passed from DataHub (reserved for future use) |
| **Returns** | `() => void` | A cleanup function that unmounts your app |

### Step 3: Configure Module Federation

Your `webpack.config.js` must include the Module Federation plugin:

```javascript
const { ModuleFederationPlugin } = require('webpack').container;

module.exports = {
    // ...
    plugins: [
        new ModuleFederationPlugin({
            // Unique name for your MFE (no spaces, alphanumeric + camelCase)
            name: 'myCustomMFE',
            
            // This file will be loaded by DataHub
            filename: 'remoteEntry.js',
            
            // Expose your mount function
            exposes: {
                './mount': './src/mount.tsx',
            },
            
            // Share React to avoid duplicate copies
            shared: {
                react: { singleton: true, requiredVersion: '^18.0.0' },
                'react-dom': { singleton: true, requiredVersion: '^18.0.0' },
            },
        }),
    ],
};
```

### Step 4: Build and Deploy

```bash
# Build for production
npm run build
# or
yarn build

# Deploy the 'dist' folder to your hosting provider
```

Your deployed app should be accessible at a URL like:
- `https://your-domain.com/my-mfe/remoteEntry.js`

### Step 5: Configure in DataHub

Contact your DataHub administrator to add your MFE configuration:

```yaml
microFrontends:
    - id: MyCustomMFE
      label: My Custom App
      path: /my-custom-app
      remoteEntry: https://your-domain.com/my-mfe/remoteEntry.js
      module: myCustomMFE/mount
      flags:
          enabled: true
          showInNav: true
      navIcon: Rocket  # See "Available Icons" section
```

## Configuration Reference

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique identifier (alphanumeric, no spaces) |
| `label` | Yes | Display name shown in navigation |
| `path` | Yes | URL path (must start with `/`) |
| `remoteEntry` | Yes | Full URL to your `remoteEntry.js` file |
| `module` | Yes | Format: `{federationName}/mount` |
| `flags.enabled` | Yes | `true` to enable, `false` to disable |
| `flags.showInNav` | Yes | `true` to show in navigation menu |
| `navIcon` | Yes | Icon name (see Available Icons) |

### Available Icons

DataHub uses [Phosphor Icons](https://phosphoricons.com/). Common options include:

| Icon Name | Description |
|-----------|-------------|
| `HandWaving` | Waving hand 👋 |
| `Rocket` | Rocket 🚀 |
| `ChartLine` | Line chart 📈 |
| `Database` | Database 🗄️ |
| `Gear` | Settings ⚙️ |
| `Lightning` | Lightning bolt ⚡ |
| `MagnifyingGlass` | Search 🔍 |
| `Users` | Users/Team 👥 |
| `Shield` | Security 🛡️ |
| `Code` | Code/Developer 💻 |

Browse all icons at [phosphoricons.com](https://phosphoricons.com/).

## Framework Examples

### React (Recommended)

```typescript
// src/mount.tsx
import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';

export function mount(container: HTMLElement): () => void {
    const root = createRoot(container);
    root.render(<App />);
    return () => root.unmount();
}
```

### Vue 3

```typescript
// src/mount.ts
import { createApp } from 'vue';
import App from './App.vue';

export function mount(container: HTMLElement): () => void {
    const app = createApp(App);
    app.mount(container);
    return () => app.unmount();
}
```

### Angular

```typescript
// src/mount.ts
import { platformBrowserDynamic } from '@angular/platform-browser-dynamic';
import { AppModule } from './app/app.module';

export function mount(container: HTMLElement): () => void {
    const platform = platformBrowserDynamic();
    const moduleRef = platform.bootstrapModule(AppModule);
    
    return () => {
        moduleRef.then(ref => ref.destroy());
    };
}
```

### Svelte

```typescript
// src/mount.ts
import App from './App.svelte';

export function mount(container: HTMLElement): () => void {
    const app = new App({ target: container });
    return () => app.$destroy();
}
```

### Vanilla JavaScript

```javascript
// src/mount.js
export function mount(container) {
    container.innerHTML = `
        <div class="my-app">
            <h1>Hello from Vanilla JS!</h1>
        </div>
    `;
    
    return () => {
        container.innerHTML = '';
    };
}
```

## Deployment Options

### Option 1: AWS S3 + CloudFront

1. Build your MFE: `npm run build`
2. Upload `dist/` to S3 bucket
3. Configure CloudFront distribution with proper CORS headers
4. Use CloudFront URL as `remoteEntry`

```bash
# Example AWS CLI deployment
aws s3 sync dist/ s3://my-mfe-bucket/my-app/ --delete
```

### Option 2: Vercel

```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
vercel --prod
```

Add `vercel.json` for CORS headers:

```json
{
    "headers": [
        {
            "source": "/(.*)",
            "headers": [
                { "key": "Access-Control-Allow-Origin", "value": "*" },
                { "key": "Access-Control-Allow-Methods", "value": "GET, OPTIONS" }
            ]
        }
    ]
}
```

### Option 3: Netlify

Create `netlify.toml`:

```toml
[build]
  publish = "dist"
  command = "npm run build"

[[headers]]
  for = "/*"
  [headers.values]
    Access-Control-Allow-Origin = "*"
```

### Option 4: Self-Hosted (Nginx)

```nginx
server {
    listen 443 ssl;
    server_name mfe.your-domain.com;

    location /my-mfe/ {
        alias /var/www/my-mfe/dist/;
        
        # CORS headers
        add_header Access-Control-Allow-Origin *;
        add_header Access-Control-Allow-Methods 'GET, OPTIONS';
        
        # Cache control for remoteEntry.js (short cache)
        location ~* remoteEntry\.js$ {
            add_header Cache-Control "no-cache, must-revalidate";
        }
    }
}
```

## Security Requirements

### CORS Configuration

Your MFE hosting must allow cross-origin requests from your DataHub domain:

```
Access-Control-Allow-Origin: https://your-datahub-instance.acryl.io
Access-Control-Allow-Methods: GET, OPTIONS
Access-Control-Allow-Headers: Content-Type
```

For development, you can use `*` but **always restrict in production**.

### HTTPS

All production MFEs **must be served over HTTPS**. DataHub will not load MFEs from insecure HTTP URLs in production.

### Content Security Policy

If your organization uses CSP, ensure your MFE domain is allowlisted:

```
script-src 'self' https://your-mfe-domain.com;
```

## Troubleshooting

### MFE Not Loading

1. **Check browser console** for errors
2. **Verify remoteEntry.js is accessible**: Visit the URL directly
3. **Check CORS headers**: Use browser DevTools Network tab
4. **Verify module name matches**: The `module` config must match your `ModuleFederationPlugin.name`

### "mount is not a function" Error

Ensure your mount function is:
1. Exported as a **named export**: `export function mount(...)`
2. Exposed correctly in webpack: `'./mount': './src/mount.tsx'`

### Styles Not Applied

If using CSS/SCSS, ensure your webpack config includes appropriate loaders:

```javascript
module: {
    rules: [
        {
            test: /\.css$/,
            use: ['style-loader', 'css-loader'],
        },
    ],
}
```

### React Version Mismatch

DataHub uses React 18. Configure shared dependencies to use singleton:

```javascript
shared: {
    react: { singleton: true, requiredVersion: '^18.0.0', eager: true },
    'react-dom': { singleton: true, requiredVersion: '^18.0.0', eager: true },
}
```

## Best Practices

### Performance

- **Lazy load** heavy components
- **Minimize bundle size** - only include necessary dependencies
- **Use code splitting** for large apps
- **Cache aggressively** (except remoteEntry.js)

### User Experience

- **Match DataHub's visual style** for a seamless experience
- **Handle loading states** gracefully
- **Provide error boundaries** to catch and display errors

### Development

- **Test locally** with the standalone HTML template before integrating
- **Use TypeScript** for type safety
- **Version your remoteEntry.js** path for cache busting in production

## Example: Complete Webpack Config

```javascript
const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const { ModuleFederationPlugin } = require('webpack').container;

module.exports = (env, argv) => {
    const isProduction = argv.mode === 'production';
    
    return {
        entry: './src/index.tsx',
        mode: isProduction ? 'production' : 'development',
        
        devServer: {
            port: 3002,
            headers: {
                'Access-Control-Allow-Origin': '*',
            },
        },
        
        output: {
            // For production, use your CDN URL
            publicPath: isProduction 
                ? 'https://your-cdn.com/my-mfe/' 
                : 'http://localhost:3002/',
            path: path.resolve(__dirname, 'dist'),
            filename: '[name].[contenthash].js',
            clean: true,
        },
        
        resolve: {
            extensions: ['.tsx', '.ts', '.js'],
        },
        
        module: {
            rules: [
                {
                    test: /\.(ts|tsx)$/,
                    use: 'ts-loader',
                    exclude: /node_modules/,
                },
                {
                    test: /\.css$/,
                    use: ['style-loader', 'css-loader'],
                },
            ],
        },
        
        plugins: [
            new ModuleFederationPlugin({
                name: 'myCustomMFE',
                filename: 'remoteEntry.js',
                exposes: {
                    './mount': './src/mount.tsx',
                },
                shared: {
                    react: { 
                        singleton: true, 
                        requiredVersion: '^18.0.0',
                        eager: true,
                    },
                    'react-dom': { 
                        singleton: true, 
                        requiredVersion: '^18.0.0',
                        eager: true,
                    },
                },
            }),
            new HtmlWebpackPlugin({
                template: './public/index.html',
            }),
        ],
        
        optimization: {
            splitChunks: false, // Required for Module Federation
        },
    };
};
```

## Support

For assistance with MFE integration:

1. **Documentation**: Refer to this guide and the MFE README
2. **DataHub Community**: [Slack community](https://datahubproject.io/slack)

---

*Last updated: January 2026*

