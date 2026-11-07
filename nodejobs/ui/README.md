# Nodejobs UI

PyQt desktop shell for staged `nodejobs` UI development.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install pyqt6
```

## Run

```bash
python -m nodejobs.ui.app
```

## Run with options

```bash
python -m nodejobs.ui.app --open-tab components
python -m nodejobs.ui.app --stage-label "Current Plan Stage: Stage 11"
python -m nodejobs.ui.app --db-path /absolute/path/to/nodejobs_db
```

## Stage checkpoint runner

```bash
python -m nodejobs.ui.run_stage --stage 7
python -m nodejobs.ui.run_stage --stage 12 --open-tab jobs
python -m nodejobs.ui.run_stage --stage 12 --open-tab jobs --db-path /absolute/path/to/nodejobs_db
```

## Real mode policy

- Stages 12+ are real-backend only for Jobs/Schedules tabs.
- There is no mock fallback in Jobs/Schedules when disconnected.
- Use the top connection bar (`DB Path`, `Browse`, `Connect`) to bind to the target nodejobs DB.
