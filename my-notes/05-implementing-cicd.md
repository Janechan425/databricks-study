---
type: study-notes
status: published
tags: [databricks, data-engineer-associate, domain-5]
---
# Domain 5 — Implementing CI/CD

Sources: [Git Folders](../certifications/data-engineer-associate/05-implementing-cicd/01-git-folders.md), [Declarative Automation Bundles](../certifications/data-engineer-associate/05-implementing-cicd/02-declarative-automation-bundles.md).

## 1. Git Folders and collaboration
- **Git Folder** (formerly Repos) integrates a Git repository into Databricks workspace. Standard loop: clone → feature branch → edit → commit & push → pull request in GitHub → review → merge → deploy.
- Databricks Git UI can handle branch, commit & push, pull, merge/rebase and some conflicts; **pull requests and code reviews** live at the Git provider (GitHub).
- **Commit** saves in local repository history; **push** sends to remote; **pull** integrates remote changes. A notebook's revision history is not a substitute for Git collaboration.
- **Merge** combines branches with a merge commit where applicable; **rebase** replays your commits on a newer base and rewrites their hashes. Avoid rebasing an already-shared branch without coordination.

```bash
# Feature branch is behind main
git fetch origin
git switch feature/orders
git rebase origin/main
# if conflicts: fix in VS Code or supported UI, mark resolved, then continue
git add path/to/file.py
git rebase --continue
```

**Exam traps:** conflicts can occur during merge *or* rebase; staging `git add` marks resolution in CLI, while Git UIs can stage through buttons. Git Folder switching branches may change file paths/contents affecting jobs; check dependencies. A protected main branch requires PR rather than direct pushes.

## 2. Declarative Automation Bundles (formerly Databricks Asset Bundles)
**Bundle** = versioned project configuration and source code used to deploy Databricks resources as code (**Infrastructure as Code, IaC**). It is a deployable package definition, not a new runtime engine.

```text
data-project/
├── databricks.yml            # root bundle name / includes / targets / variables
├── resources/
│   ├── jobs.yml              # Lakeflow Job definitions
│   └── pipelines.yml         # optional pipeline definitions
├── src/                     # notebooks / Python code
├── dbt_project.yml          # if dbt is used
└── .github/workflows/        # GitHub Actions CI/CD
```

```yaml
# databricks.yml
bundle:
  name: orders-etl
include:
  - resources/*.yml
variables:
  catalog:
    default: dev
    description: Target Unity Catalog catalog
targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: dev
  prod:
    mode: production
    variables:
      catalog: prod
```

```yaml
# resources/jobs.yml (illustrative)
resources:
  jobs:
    orders_job:
      name: orders-job
      tasks:
        - task_key: run_dbt
          dbt_task:
            project_directory: .
            commands:
              - dbt deps
              - dbt build
          environment_key: default
      environments:
        - environment_key: default
          spec:
            client: "1"
```

> Note: task schema, compute support and dbt task syntax depend on the deployed Databricks runtime / CLI; validate the actual bundle and use the source lesson for working variants.

## 3. Variables, targets and overrides
- **Variable declaration**: `variables.catalog` in root YAML; **reference**: `${var.catalog}` in resource config; **target-specific override**: `targets.prod.variables.catalog`.
- Resolution precedence (higher to lower): CLI `--var` → environment `BUNDLE_VAR_<name>` → `variable-overrides.json` → target `variables` → default.
- **development** target uses dev-safe defaults/name prefixes; **production** target applies production semantics and may enforce permission/identity safeguards.
- Do not commit passwords or tokens in bundle YAML. Use secure authentication/secret handling.

## 4. CLI and GitHub Actions delivery
```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run orders_job -t dev
databricks bundle deploy -t prod
```

- **validate** checks configuration; **deploy** creates/updates workspace resources; **run** executes a deployed resource; **destroy** tears down managed resources (use cautiously).
- Example swimlane: **Developer** changes SQL/dbt source and resource YAML → **GitHub PR** reviews/tests → **GitHub Actions** authenticates as service principal → validates → deploys bundle to target → **Databricks Jobs** runs configured dbt task → **dbt models** compile SQL into Delta tables/views according to materialization.
- **dbt model** = SQL transformation with configured materialization; **dbt macro** = reusable Jinja template compiled into SQL. A bundle *deploys the job/resources* that run dbt; dbt defines transformations.

## 5. Common issues and exam tips
- Wrong environment: pass `-t dev/prod`; verify workspace host and target overrides.
- Variable ignored: check precedence, spelling, `BUNDLE_VAR_` and target-specific values.
- Prod deployment rejected: inspect run identity/permissions and target production guardrails.
- Dev schedules not firing: inspect development mode defaults and whether schedule is paused.
- Security: use GitHub Actions secrets/OIDC or supported secure identities, not embedded credentials.
- **IaaS** = Infrastructure as a Service; **IaC** = Infrastructure as Code. Don't confuse them.
- **precedence** = priority/order of resolution; **synergy** is grammatical as a noun but in technical diagrams prefer **work together / integrate / orchestrate**.
