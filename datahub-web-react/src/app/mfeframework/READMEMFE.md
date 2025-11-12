# Micro Frontend (MFE) Onboarding & Contributor Guide

## Introduction

Scalable onboarding of Micro Frontends (MFEs) in DataHub is achieved using a validated configuration schema.  
MFEs are declared in a canonical YAML file, validated at runtime, and dynamically integrated into the application’s navigation.  
With this approach, you can add, update, or remove MFEs by configuration only—**no manual code changes required**.

## Setup & Usage

1. **See `datahub-web-react/src/app/mfeframework` for all relevant code and configuration.**
2. **Declare MFEs in `mfe.config.yaml` using the schema below.**
3. **Navigation items are automatically added based on your YAML settings—no manual coding needed\***
4. **Run locally:** Point `remoteEntry` to your local dev server.
5. **Deploy:** No code changes required for new MFEs—just update the config! --> (`mfe.config.yaml`)

## Validation & Troubleshooting

- **Validation logic** `mfeConfigLoader.tsx` handles validation of `mfe.config.yaml`
    - Invalid entries are marked with `invalid: true` and an array of `errorMessages`, and are skipped in navigation and routing.
    - **Missing required fields:** Loader will log which fields are missing.
    - **Invalid types:** Loader will log type errors (e.g., non-boolean flags).
    - **No config found:** Loader will warn if the YAML file is missing or empty.

**Example errors:**

```
[MFE Loader] flags.showInNav must be boolean
[MFE Loader] flags must be an object
[MFE Loader] path must be a string starting with "/"
```

## Features

- **Config-Driven Onboarding:** Add MFEs by editing a YAML file.
- **Validation:** Ensures all required fields are present and correctly typed.
- **Dynamic Navigation:** Automatically generates navigation items for enabled MFEs.
- **Robust Error Handling:** Logs clear error messages for missing or invalid configuration.

## Configuration Schema

Declare your MFEs in a YAML file (`mfe.config.yaml`) using the following example structure:

The following schema defines all micro frontends available in the application.
Each field is **mandatory** and must conform to the specified type.

- **subNavigationMode:** boolean (true/false)
- **microFrontends:** list of MFE objects (see below)

Each MFE object (microFrontends) must include:

- **id:** string (unique identifier for the MFE)
- **label:** string (display name in navigation)
- **path:** string (route path, must start with '/')
- **remoteEntry:** string (URL to the remoteEntry.js file)
- **module:** string (module name to mount, e.g. 'appName/mount')
- **flags:** object (see below)
    - **enabled:** boolean (true to enable, false to disable)
    - **showInNav:** boolean (true to show in navigation)
- **permissions:** array of strings (required permissions for access)
- **navIcon:** string (icon name from "@phosphor-icons/react", e.g. 'Gear')

**Example icon names:** Gear, Globe, Acorn, Airplane, Alarm
**See: https://github.com/phosphor-icons/react/tree/master/src/csr**

```yaml
subNavigationMode: false
microFrontends:
    - id: example-1
      label: Example MFE Yaml Item
      path: /example-mfe-item
      remoteEntry: http://example.com/remoteEntry.js
      module: exampleApplication/mount
      flags:
          enabled: true
          showInNav: true
      permissions:
          - example_permission
      navIcon: Gear
```

## Dynamic Navigation Integration

Sidebar navigation items are generated dynamically from the validated MFE config.
You may toggle between using Sub Navigation or not:

- See above schema for: **subNavigationMode:** boolean (true/false)
    - If subNavigationMode is enabled, the navigation bar will render each MFE into a scrollable list
    - If subNavigationMode is disabled, each MFE will appear in navigation bar in order of declaration of yaml

## Common Pitfalls

- **All required fields must be present and valid.**
- **Path must start with `/`.**
- **RemoteEntry must be accessible.**
- **Icon names must be valid or provide a valid image path.**
- **Permissions array must not be empty.**
- **Local server must be running for local development.**
- **Invalid entries are logged and skipped.**

## Next Steps

- **Deployment:** No code changes required for new MFEs—just update the config!  
  _(Production deployment steps are still being finalized. Please check back for updates.)_
