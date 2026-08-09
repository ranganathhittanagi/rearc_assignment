# Process Notes - Rearc Databricks Assignment

## Architecture

```mermaid
flowchart LR
    classDef source fill:#d0f0f8,stroke:#76a5af,stroke-width:2px;
    classDef store fill:#e6e6ff,stroke:#6c5ce7,stroke-width:2px;
    classDef bronze fill:#ffe0b2,stroke:#e67e22,stroke-width:2px;
    classDef silver fill:#e8e8e8,stroke:#7f8c8d,stroke-width:2px;
    classDef gold fill:#fff9c4,stroke:#f1c40f,stroke-width:2px;
    classDef consumer fill:#d5f5e3,stroke:#27ae60,stroke-width:2px;
    classDef pipeline fill:#e6e6ff,stroke:#6c5ce7,stroke-width:2px;

    WS[/Website Data Sources/] -->|ingest| Volumes[Databricks Volumes]
    API[/API Data Sources/] -->|ingest| Volumes
    Volumes -->|raw data| Bronze[Bronze Layer]
    Bronze -->|cleanse| Silver[Silver Layer]
    Silver -->|aggregate| Gold[Gold Layer]
    Gold -->|dashboards| Dashboards((AI/BI Dashboards))
    Gold -->|explore| Genie((Genie Space))
    Pipelines[/Declarative Pipelines/] -->|orchestrates| Bronze
    Pipelines -->|orchestrates| Silver
    Pipelines -->|orchestrates| Gold

    class WS,API source
    class Volumes store
    class Bronze bronze
    class Silver silver
    class Gold gold
    class Dashboards,Genie consumer
    class Pipelines pipeline
```

- **Bronze/Silver/Gold separation** keeps raw history (Bronze), reusable cleaned/typed assets (Silver), and stakeholder-ready answers (Gold). If a downstream calculation is wrong, I can fix the Gold logic and re-run from Silver without re-landing data.
- **Bronze uses Auto Loader (`cloudFiles`)** so the pipeline is incremental by default. New BLS files or population refreshes are picked up automatically.
- **Values stay STRING in Bronze** because the source is column-delimited text with padded/odd column names. Typing happens in Silver, where expectations can fail rows cleanly.
- **PySpark is primary in Gold**, with the equivalent Spark SQL kept in comments. This makes the logic testable in a notebook and readable for stakeholders, while still leveraging DataFrame operations for joins, window functions, and aggregations.
- **Ingestion re-runs safely** by comparing the SHA/content of each fetched file to what is already in the Unity Catalog Volume. Unchanged files are skipped; removed source files are deleted locally so stale data does not stay in Bronze.

## Trade-offs (what I would change for a real client)

- **Schema drift**: today Silver tables hard-code column names. A real client would add a schema registry or `expect_*` DLT constraints and version the Bronze schemas so upstream file-format changes fail fast.
- **Data volume**: the BLS dataset is small, so a single-node ingestion job is fine. For high-volume sources I would split ingestion into multiple tasks, use Spark to parallelize downloads, and/or land files in cloud storage first.
- **Cost**: this workspace only supports serverless compute, which is easy to operate but can be expensive at scale. I would add job/pipeline schedules, cluster policies, and monitor DBUs per run.
- **Access control**: currently the Gold tables are in the `workspace` catalog. A production setup would use a dedicated catalog, separate schemas per environment, and UC grants scoped to groups (e.g., `rearc_analysts` read-only on Gold).
- **Monitoring**: I would add DLT expectations as metrics, emit job/pipeline failures to a alerting channel (email/Slack/PagerDuty), and build a small observability table tracking run IDs, row counts, and latency.

## Retrospective: hardest part to get right

- **Deploying through Databricks Asset Bundles** took the most iteration. The workspace only allowed serverless compute, so the original classic-cluster job definition had to be switched to a serverless job environment, and the ingestion script had to read its parameters as command-line arguments instead of notebook widgets.
- **Local imports from a standalone Python job** also required a small workaround. Because the job runs from a copied file rather than an installed package, I added the repo `src` folder to `sys.path` at runtime.
- The actual data-modeling questions (best year, population stats, Q01 series) were straightforward once the Bronze/Silver/Gold layers were cleanly separated.
