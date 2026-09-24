# Mise-managed Yarn 4 status

Date: 2026-09-22. This review checks whether the branch uses the Yarn 4 version pinned in
`mise.toml` across local and CI entry points.

## Current implementation

The branch pins Node 22.23.2 and Yarn 4.18.0 in `mise.toml`, uses the `node-modules` linker in
`.yarnrc.yml`, and migrates the frontend, docs, and Playwright lockfiles. On this host,
`mise exec -- yarn --version` returns `4.18.0`.

The root Gradle build disables the Node plugin's `yarnSetup` task and overrides every `YarnTask`
invocation to run `mise exec -- yarn`. This retains existing Gradle task dependencies, inputs, and
outputs while avoiding the plugin's Yarn Classic bootstrap. The frontend Markdown formatting task
ran through this path successfully.

The relevant GitHub workflows install Node and Yarn with the pinned `jdx/mise-action` before their
Yarn commands. Runtime Docker images and Kubernetes deployments consume built artifacts and do
not need a package manager.

## Remaining gaps

| Entry point                                                                   | Current behavior                                            | Needed for a guaranteed Yarn 4 invocation                                |
| ----------------------------------------------------------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------ |
| `scripts/dev/datahub_dev.py` frontend setup, frontend server, and docs server | Calls `yarn` directly                                       | Invoke `mise exec -- yarn` or ensure the wrapper activates mise.         |
| Frontend Gradle targeted lint and fix branches                                | Calls `yarn` directly through `execOps`                     | Use the same mise command as other Gradle Yarn tasks.                    |
| Remote runner bootstrap                                                       | Can run the development launcher and Gradle on another host | Install the pinned mise tools before frontend, docs, or Playwright work. |

These direct commands can use Yarn 4 in a mise-activated shell, but the branch does not guarantee
that version when the caller's `PATH` resolves another Yarn installation. The Gradle override and
lockfile migrations therefore establish the main build path, but they do not prove every entry
point works end to end. CI builds, frontend and docs development, and Playwright execution still
need validation on their respective hosts.

## Decision

Mise-managed Yarn 4 is viable and the core Gradle integration is implemented. Complete the direct
command paths above and validate the three application workflows before calling the migration
fully functional. No production Docker image or Kubernetes deployment change is required for the
package manager itself.
