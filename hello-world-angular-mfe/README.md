# Hello World Angular MFE

An Angular-based micro-frontend for DataHub demonstrating Module Federation integration.

## Quick Start

### 1. Install Dependencies

```bash
cd hello-world-angular-mfe
npm install
```

### 2. Start Development Server

```bash
npm start
```

The Angular MFE runs at `http://localhost:3004` 

### 3. Configure DataHub

Add to `datahub-frontend/conf/mfe.config.local.yaml`:

```yaml
microFrontends:
    # ... existing MFEs ...
    - id: HelloWorldAngular
      label: Hello Angular
      path: /helloworld-angular
      remoteEntry: http://localhost:3004/remoteEntry.js
      module: helloWorldAngularMFE/mount
      flags:
          enabled: true
          showInNav: true
      navIcon: Code
```

### 4. Access in DataHub

Navigate to `http://localhost:3000/mfe/helloworld-angular`

## Project Structure

```
hello-world-angular-mfe/
├── src/
│   ├── app/
│   │   ├── app.component.ts    # Main Angular component
│   │   └── app.module.ts       # Angular module
│   ├── main.ts                 # Standalone bootstrap
│   ├── mount.ts                # MFE mount function (required)
│   ├── styles.css              # Global styles
│   └── index.html              # HTML template
├── angular.json                # Angular CLI config
├── package.json                # Dependencies
├── tsconfig.json               # TypeScript config
├── webpack.config.js           # Module Federation config
└── README.md
```

## Key Concepts

### Mount Function

The `mount.ts` file exports the function that DataHub calls to render the Angular app:

```typescript
export function mount(container: HTMLElement, options: object): () => void {
    const appRoot = document.createElement('app-root');
    container.appendChild(appRoot);

    let moduleRef: NgModuleRef<AppModule> | null = null;

    platformBrowserDynamic()
        .bootstrapModule(AppModule)
        .then((ref) => {
            moduleRef = ref;
        });

    // Return cleanup function
    return () => {
        if (moduleRef) {
            moduleRef.destroy();
        }
        appRoot.remove();
    };
}
```

### Module Federation Config

The `webpack.config.js` exposes the mount function:

```javascript
new ModuleFederationPlugin({
    name: 'helloWorldAngularMFE',
    filename: 'remoteEntry.js',
    exposes: {
        './mount': './src/mount.ts',
    },
    shared: { /* Angular dependencies */ }
})
```

## Features Demonstrated

| Feature | Description |
|---------|-------------|
| Angular Component | Full Angular component with template and styles |
| Two-way Binding | Counter with increment/decrement |
| DataHub API | Fetches user info and recommendations via GraphQL |
| Internal Routing | State-based navigation within the MFE |
| DataHub Navigation | Links to DataHub pages |

## Available Scripts

| Script | Description |
|--------|-------------|
| `npm start` | Start dev server on port 3004 |
| `npm run build` | Build for production |

## Differences from React MFE

| Aspect | React MFE | Angular MFE |
|--------|-----------|-------------|
| Framework | React 18 | Angular 17 |
| Port | 3002 | 3004 |
| Mount | `createRoot().render()` | `platformBrowserDynamic().bootstrapModule()` |
| Cleanup | `root.unmount()` | `moduleRef.destroy()` |
| Module Federation | webpack directly | @angular-architects/module-federation |

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Build errors | Run `npm install` to ensure dependencies are installed |
| Module Federation errors | Ensure `@angular-architects/module-federation` is installed |
| Zone.js errors | Check that zone.js is in polyfills |
| App not loading | Verify the mount function exports correctly |
