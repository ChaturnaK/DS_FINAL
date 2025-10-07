# DS_FINAL

Distributed storage final project staging.

## Build

```bash
mvn -DskipTests package
```

## Run Local Cluster

```bash
bash scripts/run-local.sh
```

## Restart

```bash
bash scripts/restart.sh
```

## Chaos Demo

```bash
bash scripts/demo.sh
```

## Report Generation

```bash
python3 scripts/gen-report.py
```

Outputs `report.md` and (if pandoc present) `report.pdf` alongside metric CSV summaries under `metrics/`.
