# Webinar Email Migration Pipeline — Braze

A Python pipeline that migrates webinar follow-up emails from a BigQuery CSV export into Braze email templates. Each email is processed by a Gemini AI agent that applies Braze Liquid variable substitution and HTML cleanup before being pushed to Braze via the REST API.

---

## How it works

For each row in the CSV, the pipeline:

1. Sends the raw HTML to a Gemini AI agent (using the Email HTML Improvement Agent prompt) which handles Liquid variable substitution, HTML cleanup, and content quality improvements
2. Wraps the processed content in the Mindvalley production email scaffold (correct table structure, mobile styles, Braze content blocks for header/footer)
3. Creates or updates the template in Braze via the REST API

Templates are named: `masterclass_{webinar_id}_{type}_{subject_slug}`

The script is idempotent — safe to re-run. If a template already exists it will be updated, not duplicated.

---

## Requirements

- Python 3.10+
- A Braze API key with `templates.email.create`, `templates.email.update`, and `templates.email.list` permissions
- A Google AI Studio API key (Gemini)
- A BigQuery CSV export pre-filtered to active records

---

## Setup

**1. Clone the repo**
```bash
git clone https://github.com/mindvalley-ai/webinar-email-migration-pipeline
cd webinar-email-migration-pipeline
```

**2. Install dependencies**
```bash
pip3 install pandas requests python-dotenv google-genai
```

**3. Set up credentials**
```bash
cp .env.example .env
```

Open `.env` and fill in your values:
```
BRAZE_API_KEY=your_braze_api_key_here
GEMINI_API_KEY=your_gemini_api_key_here
BRAZE_BASE_URL=https://rest.iad-01.braze.com
```

> The Braze base URL depends on your instance — check your dashboard URL to confirm the correct endpoint.

**4. Add your CSV export** to the project folder. The file must contain these columns:
`id`, `webinar_id`, `subject`, `content`, `type`

Pre-filter to active records in BigQuery before exporting.

---

## Usage

**Test a single row (dry run — no Braze writes):**
```bash
python3 migrate.py --csv your_export.csv --dry-run --ids 183
```

**Full dry run across all rows:**
```bash
python3 migrate.py --csv your_export.csv --dry-run
```

**Live run — creates/updates templates in Braze:**
```bash
python3 migrate.py --csv your_export.csv
```

**Debug mode — saves cleaned HTML to file for inspection before sending:**
```bash
python3 migrate.py --csv your_export.csv --dry-run --ids 183 --debug
```

---

## Output

- `migration.log` — full run log with per-row status and any audit issues flagged by the agent
- `migration_results.json` — structured audit trail with template IDs, statuses, and agent audit output

---

## Email types supported

| Type | Description |
|---|---|
| `no-show` | Registrants who did not attend |
| `all-registrants` | All registered users |
| `attended-left-early` | Attendees who dropped off early |
| `attended-saw-dropdown` | Attendees who saw the offer |
| `timed-before-webinar` | Sent before the webinar |
| `timed-from-sign-up` | Sent relative to sign-up date |
| `customer` | Existing customers |

---

## Liquid variable mapping

Legacy dynamic parameters are automatically converted to Braze Liquid syntax:

| Legacy | Braze |
|---|---|
| `{{ user_name }}` | `{{${first_name}}}` |
| `{{ webinar_time }}` | `{{context.${webinar_time}}}` |
| `{{ webinar_live_link }}` | `{{context.${webinar_live_link}}}` |
| `{{ webinar_link }}` | `{{context.${webinar_link}}}` |
| `{{ webinar_replay_link }}` | `{{context.${webinar_replay_link}}}` |
| `{{ webinar_replay }}` | `{{context.${webinar_replay}}}` |
| `{{ webinar_date_weekday }}` | `{{context.${webinar_date_weekday}}}` |
| `{{ webinar_date_day }}` | `{{context.${webinar_date_day}}}` |
| `{{ webinar_date_month_name }}` | `{{context.${webinar_date_month_name}}}` |
| `{{ google_calendar_url }}` | `{{context.${google_calendar_url}}}` |
| `{{ icalendar_url }}` | `{{context.${icalendar_url}}}` |
| `{{ outlookonline_url }}` | `{{context.${outlookonline_url}}}` |
| `{{ unsubscribe_link }}` | Removed — handled by Braze footer content block |
| `viewInBrowserUrl` | Removed — handled by Braze view-in-browser content block |

---

## Braze content blocks used

The following Braze content blocks are injected automatically into every template:

| Block | Tag |
|---|---|
| View in browser | `{{content_blocks.${view_in_browser-en} \| id: 'cb7'}}` |
| Mindvalley logo | `{{content_blocks.${header_mindvalley_logo} \| id: 'cb3'}}` |
| Footer left | `{{content_blocks.${footer_left_side} \| id: 'cb5'}}` |
| Footer right | `{{content_blocks.${footer_right_side_pref_center} \| id: 'cb6'}}` |

These must exist in your Braze instance before sending.
