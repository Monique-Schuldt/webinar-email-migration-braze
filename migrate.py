"""
Webinar Email → Braze Migration Pipeline
=========================================
Reads a BigQuery CSV export, runs each email through the HTML Improvement
Agent (Claude, Batch Mode B), then POSTs the result to Braze.

Pipeline per row:
  1. Build JSON payload (Batch Mode B)
  2. POST to Claude API → get cleaned HTML + audit log
  3. Assert image URLs intact
  4. Upsert to Braze (create or update)

Usage:
    python migrate.py --csv path/to/export.csv
    python migrate.py --csv path/to/export.csv --dry-run
    python migrate.py --csv path/to/export.csv --ids 183 192
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from typing import Optional

from google import genai
import pandas as pd
import requests
from dotenv import load_dotenv

# ── Environment ────────────────────────────────────────────────────────────────
load_dotenv()

BRAZE_API_KEY  = os.environ["BRAZE_API_KEY"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
BRAZE_BASE_URL = os.getenv("BRAZE_BASE_URL", "https://rest.iad-01.braze.com")

GEMINI_MODEL     = "gemini-2.5-flash"
BRAZE_RATE_SLEEP = 0.3  # seconds between Braze API calls

# ── HTML Improvement Agent system prompt ───────────────────────────────────────
# Full prompt lives here so the script is self-contained.
AGENT_SYSTEM_PROMPT = r"""# Email HTML Improvement Agent — System Prompt

---

## IDENTITY

You are an expert email HTML engineer specialising in cross-client rendering, deliverability, and data compliance. You receive raw email HTML and return a fully corrected, production-ready version aligned to Mindvalley's Braze email standards. You are precise, deterministic, and non-destructive: you fix what is broken, improve what is weak, and never invent content.

---

## INPUT FORMAT

You accept input in two modes. Detect which mode applies automatically.

**Mode A — Interactive (single email)**
The user pastes raw HTML directly into the message, or describes a problem with an email. Respond with the full corrected HTML wrapped in a fenced `html` code block, followed by a structured audit summary (see OUTPUT FORMAT below).

**Mode B — Batch (script/API)**
Input is a JSON object:

```json
{
  "mode": "batch",
  "filename": "webinar-email-001.html",
  "html": "<raw HTML string>",
  "options": {
    "brand": "Mindvalley",
    "primary_color": "#7A12D4",
    "text_color": "#0F131A",
    "background_color": "#F3F4F6",
    "card_background": "#FFFFFF",
    "font_body": "Verdana, Arial, Sans-serif",
    "font_size_body": "16px",
    "font_size_heading": "20px",
    "font_size_subtext": "14px",
    "liquid_vars": [
      "{{ user_name }}", "{{ webinar_date_weekday }}", "{{ webinar_date_day }}",
      "{{ webinar_date_month_name }}", "{{ webinar_time }}", "{{ webinar_live_link }}",
      "{{ webinar_link }}", "{{ webinar_replay_link }}", "{{ webinar_replay }}",
      "{{ google_calendar_url }}", "{{ icalendar_url }}", "{{ outlookonline_url }}"
    ]
  }
}
```

If `options` is omitted in batch mode, apply all rules using the Mindvalley defaults documented in this prompt.

---

## PRODUCTION BASELINE — MINDVALLEY STANDARD

### Layout

- **Outer wrapper**: full-width `<table>` with `background-color: #F3F4F6`.
- **Inner content table**: `width="580"`, `max-width: 580px`, centred with `margin: 0 auto`.
- **Card shape**: `border-radius: 16px 16px 0 0` on first content row, `border-radius: 0 0 16px 16px` on last content row. Rows in between have `border-radius: 0`.
- **Card background**: `#FFFFFF` for all content rows.
- **Top spacer**: 24px spacer row at the very top before the first content card.
- **Content padding**: `padding-left: 60px; padding-right: 60px` on desktop. Reduces to `24px` on mobile via media query.

### Typography

- **Font family**: `Verdana, Arial, Sans-serif` on all elements.
- **Body text**: `font-size: 16px; font-weight: 400; line-height: 1.5; color: #0F131A;`
- **Headings**: `font-size: 20px; font-weight: 700; line-height: 30px; color: #0F131A;`
- **Secondary text**: `font-size: 14px; font-weight: 400; line-height: 1.5; color: #0F131A;`
- Always pair `line-height` with `mso-line-height-alt` in px.

### Colour Palette

| Purpose | Hex |
|---|---|
| Primary brand / CTA buttons / accent links | `#7A12D4` |
| Primary text | `#0F131A` |
| Secondary text | `#292D38` |
| Page background | `#F3F4F6` |
| Card background | `#FFFFFF` |
| Info card background | `#F3F4F6` |
| Button text | `#FFFFFF` |

### CTA Buttons

Pill-shaped bulletproof VML pattern. Do not add buttons where the original uses text links. Only upgrade existing buttons.

### Images

- Max width: `460px` inside a `max-width: 460px` div wrapper.
- All images: `style="display:block;height:auto;border:0;width:100%"` with `width="460"` attribute.
- All images must have descriptive `alt` and `title` attributes.
- Never change image `src` URLs.

---

## CORE RULES — ALWAYS APPLY

### 1. Document Structure

- Ensure `<!DOCTYPE html>` is the very first line.
- Opening `<html>` tag must include VML namespaces and `lang` attribute:
  ```html
  <html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" lang="{{accessibility_language}}">
  ```
- Add `<meta charset="UTF-8">`, `<meta name="viewport" content="width=device-width,initial-scale=1">`, and `<meta http-equiv="Content-Type" content="text/html; charset=utf-8">` in `<head>`.
- Add `<meta name="format-detection" content="telephone=no, date=no, address=no, email=no">`.
- Add `<title>` — derive from first heading or topic if not present.
- Add MSO Word document and Office settings blocks in `<head>`.

### 2. CSS Placement and Inlining

- Move any `<style>` blocks found inside `<body>` into `<head>`.
- Inline all visual CSS on individual elements as `style=""` attributes.
- Never use CSS shorthand in inline styles for Outlook.
- `mso-table-lspace: 0; mso-table-rspace: 0` on every `<table>`.

### 3. Table-Based Layout

- All layout uses `<table>` — never `<div>` with flexbox or grid.
- All tables: `role="presentation"`, `border="0"`, `cellpadding="0"`, `cellspacing="0"`.

### 4. Spacing

- Replace `<tr><td>&nbsp;</td></tr>` spacer rows with:
  `<div class="spacer_block" style="height:Npx;line-height:Npx;font-size:1px">&#8202;</div>`
- Replace `<br>` tags used for spacing with padding on adjacent elements.

### 5. Preheader Text

- If absent, add immediately after `<body>` opens (max 90 characters, colour `#F3F4F6` so it is invisible).

### 6. Mobile Responsive Block

- Required `@media (max-width:600px)` block must be present in `<head>` `<style>`.

---

## BRAZE CONTENT BLOCKS

CRITICAL: These are mandatory replacements. You MUST insert the exact Braze content block tags listed below. Never hardcode logo HTML, footer HTML, or view-in-browser links — always use the tags. Hardcoding these elements is a critical error.

### View in Browser
- Remove ALL existing view-in-browser links or rows.
- Insert as a `<p style="margin:0">` in the first content row: `{{content_blocks.${view_in_browser-en} | id: 'cb7'}}`

### Logo
- Remove ALL existing logo `<img>` tags or logo rows.
- Insert as a `<p style="margin:0">` immediately after the view-in-browser block: `{{content_blocks.${header_mindvalley_logo} | id: 'cb3'}}`

### Footer
- Remove the ENTIRE existing footer — every row containing unsubscribe links, physical address, social media icons, preference centre links, or legal text.
- Replace with ONLY these two content block tags in a two-column row. Do NOT hardcode any footer HTML whatsoever:
  - Left column: `{{content_blocks.${footer_left_side} | id: 'cb5'}}`
  - Right column: `{{content_blocks.${footer_right_side_pref_center} | id: 'cb6'}}`

---

## LIQUID VARIABLE MAPPING

Replace all legacy variable formats with canonical Braze custom attribute format.

| Legacy variable | Braze variable |
|---|---|
| `{{ user_name }}` | `{{${first_name} \| default: 'there'}}` |
| `{{ webinar_time }}` | `{{${webinar_time}}}` |
| `{{ webinar_live_link }}` | `{{${webinar_live_link}}}` |
| `{{ webinar_link }}` | `{{${webinar_link}}}` |
| `{{ webinar_replay_link }}` | `{{${webinar_replay_link}}}` |
| `{{ webinar_replay }}` | `{{${webinar_replay}}}` |
| `{{ unsubscribe_link }}` | Remove — handled by `{{content_blocks.${footer_left_side}}}` |
| `{{ webinar_date_weekday }}` | `{{${webinar_date_weekday}}}` |
| `{{ webinar_date_day }}` | `{{${webinar_date_day}}}` |
| `{{ webinar_date_month_name }}` | `{{${webinar_date_month_name}}}` |
| `{{ google_calendar_url }}` | `{{${google_calendar_url}}}` |
| `{{ icalendar_url }}` | `{{${icalendar_url}}}` |
| `{{ outlookonline_url }}` | `{{${outlookonline_url}}}` |
| `viewInBrowserUrl` (any form) | Remove — handled by `{{content_blocks.${view_in_browser-en}}}` |

---

## DATA COMPLIANCE RULES

- **Reason for receiving**: The exact text must be: "You are receiving this email because you signed up for a Mindvalley Masterclass." Add or replace any existing reason-for-receiving statement with this exact copy. Place it as a centred italic paragraph immediately after the Mindvalley logo block and before the first line of email copy. Use 12px Verdana, colour `#0F131A`.
- Audit href URLs for PII in query parameters — flag as "critical" if found.
- Flag tracking pixels under "warnings".

## DELIVERABILITY RULES

Flag violations as "critical". Do not alter content — flag for sender review.

## WHAT NOT TO CHANGE

- Do not alter editorial content, messaging, or email structure beyond what rules require.
- Do not change image `src` URLs.
- Do not change subject lines.
- Do not reorder content sections.

---

## OUTPUT FORMAT

### Batch Mode (Mode B)

Return your response in EXACTLY this format:

===BODY===
[ONLY the inner HTML content — paragraphs, images, links, and CTAs from the email body.
Do NOT wrap in any <table> or <td> — return only the raw inner HTML elements like <p>, <a>, <img>, <div>.
Do NOT include: <!DOCTYPE>, <html>, <head>, <body>, <table> wrappers, the logo, view-in-browser, reason-for-receiving, or footer.
ONLY return the actual email copy content that goes inside a <td> cell.]
===PREHEADER===
[A single line of preheader text, max 90 characters, derived from the email content]
===AUDIT===
[valid JSON with keys: "status", "critical", "warnings", "improvements", "unchanged"]

No markdown fences. No prose outside these sections.
"""

# ── Logging ────────────────────────────────────────────────────────────────────
def setup_logging(log_file: str = "migration.log") -> logging.Logger:
    logger = logging.getLogger("braze_migration")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


logger = setup_logging()


# ── Helpers ────────────────────────────────────────────────────────────────────

def slugify(text: str, max_len: int = 60) -> str:
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = text.strip().replace(" ", "_")
    text = re.sub(r"_+", "_", text)
    return text[:max_len]


def template_name(webinar_id: int, email_type: str, subject: str) -> str:
    return f"masterclass_{webinar_id}_{email_type}_{slugify(subject)}"


def assert_images_intact(original_html: str, processed_html: str, row_id: int) -> None:
    original_srcs = set(re.findall(r'src=["\']([^"\']+)["\']', original_html))
    processed_srcs = set(re.findall(r'src=["\']([^"\']+)["\']', processed_html))
    lost   = original_srcs - processed_srcs
    gained = processed_srcs - original_srcs
    if lost or gained:
        logger.warning(
            "[row %d] Image src mismatch (review before sending) — "
            "lost: %s  gained: %s", row_id, lost or "none", gained or "none"
        )


def enforce_content_blocks(html: str, row_id: int) -> str:
    """
    Python-side guarantee that Braze content block tags are present.
    Runs after the Gemini agent in case it hardcoded footer HTML instead of using tags.
    """
    footer_left  = "{{content_blocks.${footer_left_side} | id: 'cb5'}}"
    footer_right = "{{content_blocks.${footer_right_side_pref_center} | id: 'cb6'}}"

    if footer_left not in html or footer_right not in html:
        logger.debug("[row %d] Footer content blocks missing — injecting", row_id)

        footer_html = """
    <table class="row row-footer" align="center" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;">
        <tbody><tr><td>
            <table class="row-content stack" align="center" border="0" cellpadding="0" cellspacing="0" role="presentation"
                style="mso-table-lspace:0;mso-table-rspace:0;background-color:#fff;border-radius:0 0 16px 16px;color:#000;width:580px;margin:0 auto" width="580">
                <tbody><tr>
                    <td class="column column-1" width="50%" style="mso-table-lspace:0;mso-table-rspace:0;font-weight:400;text-align:left;padding-bottom:24px;padding-top:24px;vertical-align:top;">
                        <table class="paragraph_block block-1" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;word-break:break-word;">
                            <tr><td class="pad"><div style="color:#0f131a;direction:ltr;font-family:Verdana,Arial,Sans-serif;font-size:16px;font-weight:400;letter-spacing:0;text-align:center;">
                                <p style="margin:0">{{content_blocks.${footer_left_side} | id: 'cb5'}}</p>
                            </div></td></tr>
                        </table>
                    </td>
                    <td class="column column-2" width="50%" style="mso-table-lspace:0;mso-table-rspace:0;font-weight:400;text-align:left;padding-bottom:24px;padding-top:24px;vertical-align:top;">
                        <table class="paragraph_block block-1" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;word-break:break-word;">
                            <tr><td class="pad"><div style="color:#0f131a;direction:ltr;font-family:Verdana,Arial,Sans-serif;font-size:16px;font-weight:400;letter-spacing:0;text-align:center;">
                                <p style="margin:0">{{content_blocks.${footer_right_side_pref_center} | id: 'cb6'}}</p>
                            </div></td></tr>
                        </table>
                    </td>
                </tr></tbody>
            </table>
        </td></tr></tbody>
    </table>
    <table class="row" align="center" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0">
        <tbody><tr><td>
            <table class="row-content stack" align="center" border="0" cellpadding="0" cellspacing="0" role="presentation"
                style="mso-table-lspace:0;mso-table-rspace:0;background-color:#f3f4f6;width:580px;margin:0 auto" width="580">
                <tbody><tr>
                    <td class="column column-1" width="100%" style="mso-table-lspace:0;mso-table-rspace:0;padding-bottom:5px;padding-top:5px;vertical-align:top">
                        <div class="spacer_block" style="height:24px;line-height:24px;font-size:1px">&#8202;</div>
                    </td>
                </tr></tbody>
            </table>
        </td></tr></tbody>
    </table>
</td></tr></tbody></table>
</body>
</html>"""

        # Find footer start by looking for common footer content signals
        footer_signals = ['Unsubscribe', 'unsubscribe', 'Privacy Policy', 'Mindvalley Inc', 'Palo Alto']
        cut_pos = None
        for signal in footer_signals:
            idx = html.lower().rfind(signal.lower())
            if idx != -1:
                table_start = html.rfind('<table', 0, idx)
                if table_start != -1:
                    if cut_pos is None or table_start < cut_pos:
                        cut_pos = table_start

        if cut_pos:
            html = html[:cut_pos] + footer_html
            logger.debug("[row %d] Footer replaced with content block tags", row_id)
        else:
            html = html.replace('</body>', footer_html)
            logger.debug("[row %d] Footer injected before </body>", row_id)

    return html


def wrap_in_production_scaffold(body_html: str, preheader: str) -> str:
    """
    Wraps processed body content in the Mindvalley production email scaffold.
    This guarantees correct table structure, mobile styles, and content block
    placement regardless of what Gemini produces.
    """
    return f"""<!DOCTYPE html>
<html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" lang="{{{{accessibility_language}}}}">
<head>
    <title></title>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <meta name="format-detection" content="telephone=no, date=no, address=no, email=no">
    <!--[if mso]>
    <xml>
        <w:WordDocument xmlns:w="urn:schemas-microsoft-com:office:word">
            <w:DontUseAdvancedTypographyReadingMail/>
        </w:WordDocument>
        <o:OfficeDocumentSettings>
            <o:PixelsPerInch>96</o:PixelsPerInch>
            <o:AllowPNG/>
        </o:OfficeDocumentSettings>
    </xml>
    <![endif]-->
    <style>
        * {{ box-sizing: border-box; }}
        body {{ margin: 0; padding: 0; -webkit-text-size-adjust: none; text-size-adjust: none; background-color: #f3f4f6; }}
        a[x-apple-data-detectors] {{ color: inherit !important; text-decoration: inherit !important; }}
        #MessageViewBody a {{ color: inherit; text-decoration: none; }}
        p {{ line-height: inherit; }}
        .desktop_hide, .desktop_hide table {{ mso-hide: all; display: none; max-height: 0; overflow: hidden; }}
        .image_block img + div {{ display: none; }}
        sub, sup {{ font-size: 75%; line-height: 0; }}
        @media (max-width:600px) {{
            .mobile_hide {{ display: none; }}
            .row-content {{ width: 100% !important; }}
            .stack .column {{ width: 100%; display: block; }}
            .mobile_hide {{ min-height: 0; max-height: 0; max-width: 0; overflow: hidden; font-size: 0; }}
            .desktop_hide, .desktop_hide table {{ display: table !important; max-height: none !important; }}
            .row-content .column {{ padding-left: 24px !important; padding-right: 24px !important; }}
            img {{ max-width: 100% !important; height: auto !important; }}
        }}
    </style>
</head>
<body style="background-color:#f3f4f6;margin:0;padding:0;-webkit-text-size-adjust:none;text-size-adjust:none;">
    <div style="display:none;max-height:0;overflow:hidden;mso-hide:all;font-size:1px;color:#F3F4F6;line-height:1px;">
        {preheader}&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;
    </div>
    <table class="nl-container" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;background-color:#f3f4f6;">
        <tbody><tr><td>

            <!-- Top spacer -->
            <table class="row row-1" align="center" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;">
                <tbody><tr><td>
                    <table class="row-content stack" align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;background-color:#f3f4f6;width:580px;margin:0 auto;" width="580">
                        <tbody><tr>
                            <td class="column column-1" width="100%" style="mso-table-lspace:0;mso-table-rspace:0;padding-top:5px;padding-bottom:5px;vertical-align:top;">
                                <div class="spacer_block block-1" style="height:24px;line-height:24px;font-size:1px;">&#8202;</div>
                            </td>
                        </tr></tbody>
                    </table>
                </td></tr></tbody>
            </table>

            <!-- Header row: view in browser + logo + reason for receiving -->
            <table class="row row-2" align="center" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;">
                <tbody><tr><td>
                    <table class="row-content stack" align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;background-color:#ffffff;border-radius:16px 16px 0 0;color:#000;width:580px;margin:0 auto;" width="580">
                        <tbody><tr>
                            <td class="column column-1" width="100%" style="mso-table-lspace:0;mso-table-rspace:0;padding-left:60px;padding-right:60px;padding-top:16px;padding-bottom:16px;vertical-align:top;">
                                <table class="paragraph_block block-1" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;word-break:break-word;">
                                    <tr><td class="pad" style="text-align:center;">
                                        <div style="color:#0f131a;font-family:Verdana,Arial,Sans-serif;font-size:16px;text-align:center;">
                                            <p style="margin:0;">{{{{content_blocks.${{view_in_browser-en}} | id: 'cb7'}}}}</p>
                                        </div>
                                    </td></tr>
                                </table>
                                <table class="paragraph_block block-2" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;word-break:break-word;">
                                    <tr><td class="pad" style="padding-top:16px;padding-bottom:16px;text-align:center;">
                                        <div style="color:#0f131a;font-family:Verdana,Arial,Sans-serif;font-size:16px;text-align:center;">
                                            <p style="margin:0;">{{{{content_blocks.${{header_mindvalley_logo}} | id: 'cb3'}}}}</p>
                                        </div>
                                    </td></tr>
                                </table>
                                <table class="paragraph_block block-3" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;word-break:break-word;">
                                    <tr><td class="pad" style="padding-bottom:16px;text-align:center;">
                                        <div style="color:#0f131a;font-family:Verdana,Arial,Sans-serif;font-size:12px;line-height:1.5;mso-line-height-alt:18px;text-align:center;">
                                            <p style="margin:0;"><em>You are receiving this email because you signed up for a Mindvalley Masterclass.</em></p>
                                        </div>
                                    </td></tr>
                                </table>
                            </td>
                        </tr></tbody>
                    </table>
                </td></tr></tbody>
            </table>

            <!-- Body content rows (injected by Gemini, wrapped in white card) -->
            <table class="row row-3" align="center" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;">
                <tbody><tr><td>
                    <table class="row-content stack" align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;background-color:#ffffff;width:580px;margin:0 auto;" width="580">
                        <tbody><tr>
                            <td class="column column-1" width="100%" style="mso-table-lspace:0;mso-table-rspace:0;padding-left:60px;padding-right:60px;padding-top:24px;padding-bottom:24px;vertical-align:top;">
                                {body_html}
                            </td>
                        </tr></tbody>
                    </table>
                </td></tr></tbody>
            </table>

            <!-- Footer row -->
            <table class="row row-footer" align="center" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;">
                <tbody><tr><td>
                    <table class="row-content stack" align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;background-color:#ffffff;border-radius:0 0 16px 16px;color:#000;width:580px;margin:0 auto;" width="580">
                        <tbody><tr>
                            <td class="column column-1" width="50%" style="mso-table-lspace:0;mso-table-rspace:0;font-weight:400;text-align:left;padding-bottom:24px;padding-top:24px;vertical-align:top;">
                                <table class="paragraph_block block-1" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;word-break:break-word;">
                                    <tr><td class="pad"><div style="color:#0f131a;direction:ltr;font-family:Verdana,Arial,Sans-serif;font-size:16px;font-weight:400;letter-spacing:0;text-align:center;">
                                        <p style="margin:0;">{{{{content_blocks.${{footer_left_side}} | id: 'cb5'}}}}</p>
                                    </div></td></tr>
                                </table>
                            </td>
                            <td class="column column-2" width="50%" style="mso-table-lspace:0;mso-table-rspace:0;font-weight:400;text-align:left;padding-bottom:24px;padding-top:24px;vertical-align:top;">
                                <table class="paragraph_block block-1" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;word-break:break-word;">
                                    <tr><td class="pad"><div style="color:#0f131a;direction:ltr;font-family:Verdana,Arial,Sans-serif;font-size:16px;font-weight:400;letter-spacing:0;text-align:center;">
                                        <p style="margin:0;">{{{{content_blocks.${{footer_right_side_pref_center}} | id: 'cb6'}}}}</p>
                                    </div></td></tr>
                                </table>
                            </td>
                        </tr></tbody>
                    </table>
                </td></tr></tbody>
            </table>

            <!-- Bottom spacer -->
            <table class="row" align="center" width="100%" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;">
                <tbody><tr><td>
                    <table class="row-content stack" align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="mso-table-lspace:0;mso-table-rspace:0;background-color:#f3f4f6;width:580px;margin:0 auto;" width="580">
                        <tbody><tr>
                            <td class="column column-1" width="100%" style="mso-table-lspace:0;mso-table-rspace:0;padding-top:5px;padding-bottom:5px;vertical-align:top;">
                                <div class="spacer_block block-1" style="height:24px;line-height:24px;font-size:1px;">&#8202;</div>
                            </td>
                        </tr></tbody>
                    </table>
                </td></tr></tbody>
            </table>

        </td></tr></tbody>
    </table>
</body>
</html>"""


def load_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    required = {"id", "webinar_id", "subject", "content", "type"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")
    logger.info("Loaded %d rows from %s", len(df), csv_path)
    return df


# ── HTML Improvement Agent ─────────────────────────────────────────────────────

def run_html_agent(html: str, filename: str, row_id: int) -> tuple[str, dict]:
    """
    Send HTML to the Improvement Agent via Claude API (Batch Mode B).
    Returns (cleaned_html, audit_dict).
    """
    client = genai.Client(api_key=GEMINI_API_KEY)

    payload = json.dumps({
        "mode": "batch",
        "filename": filename,
        "html": html,
    })

    logger.debug("[row %d] Sending to HTML agent (%d chars)", row_id, len(html))

    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=payload,
        config=genai.types.GenerateContentConfig(
            system_instruction=AGENT_SYSTEM_PROMPT,
            max_output_tokens=16000,
        ),
    )
    raw = response.text.strip()

    # Parse response sections
    if "===BODY===" not in raw:
        raise RuntimeError(
            f"[row {row_id}] Agent response missing ===BODY=== delimiter. Raw: {raw[:300]}"
        )

    # Extract body content
    after_body = raw.split("===BODY===", 1)[1]
    if "===PREHEADER===" in after_body:
        body_part, rest = after_body.split("===PREHEADER===", 1)
    else:
        body_part = after_body
        rest = ""

    body_html = body_part.strip()

    # Extract preheader
    preheader = ""
    if rest and "===AUDIT===" in rest:
        preheader_part, audit_part = rest.split("===AUDIT===", 1)
        preheader = preheader_part.strip()
    elif rest:
        preheader = rest.split("===AUDIT===")[0].strip()
        audit_part = ""
    else:
        audit_part = ""

    if not body_html:
        raise RuntimeError(f"[row {row_id}] Agent returned empty body content")

    # Wrap body in production scaffold
    final_html = wrap_in_production_scaffold(body_html, preheader)

    # Parse audit JSON if present
    audit: dict = {}
    if audit_part.strip():
        audit_raw = audit_part.strip()
        audit_raw = re.sub(r"^```json\s*", "", audit_raw, flags=re.MULTILINE)
        audit_raw = re.sub(r"\s*```$", "", audit_raw)
        try:
            audit = json.loads(audit_raw)
        except json.JSONDecodeError:
            logger.debug("[row %d] Could not parse audit JSON, continuing", row_id)

    status = audit.get("status", "improved")
    logger.debug("[row %d] Agent status: %s", row_id, status)

    for issue in audit.get("critical", []):
        logger.warning("[row %d] CRITICAL: %s", row_id, issue)
    for issue in audit.get("warnings", []):
        logger.debug("[row %d] WARNING: %s", row_id, issue)

    return final_html, audit


# ── Braze API ──────────────────────────────────────────────────────────────────

def _braze_headers() -> dict:
    return {
        "Authorization": f"Bearer {BRAZE_API_KEY}",
        "Content-Type":  "application/json",
    }


def get_existing_templates() -> dict[str, str]:
    """Fetch all Braze email templates → {template_name: template_id}."""
    url     = f"{BRAZE_BASE_URL}/templates/email/list"
    results: dict[str, str] = {}
    offset  = 1
    limit   = 100

    while True:
        resp = requests.get(
            url, headers=_braze_headers(),
            params={"count": limit, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        templates = resp.json().get("templates", [])
        for t in templates:
            results[t["template_name"]] = t["email_template_id"]
        if len(templates) < limit:
            break
        offset += limit
        time.sleep(BRAZE_RATE_SLEEP)

    logger.info("Found %d existing Braze templates", len(results))
    return results


def create_template(name: str, subject: str, html_body: str) -> str:
    resp = requests.post(
        f"{BRAZE_BASE_URL}/templates/email/create",
        headers=_braze_headers(),
        json={"template_name": name, "subject": subject, "body": html_body},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("email_template_id", "")


def update_template(template_id: str, name: str, subject: str, html_body: str) -> None:
    resp = requests.post(
        f"{BRAZE_BASE_URL}/templates/email/update",
        headers=_braze_headers(),
        json={
            "email_template_id": template_id,
            "template_name":     name,
            "subject":           subject,
            "body":              html_body,
        },
        timeout=30,
    )
    resp.raise_for_status()


# ── Main pipeline ──────────────────────────────────────────────────────────────

def process_row(
    row: pd.Series,
    existing_templates: dict[str, str],
    dry_run: bool = False,
    debug: bool = False,
) -> dict:
    row_id     = int(row["id"])
    webinar_id = int(row["webinar_id"])
    email_type = str(row["type"])
    subject    = str(row["subject"])
    html       = str(row["content"])
    tname      = template_name(webinar_id, email_type, subject)
    filename   = f"{tname}.html"

    result: dict = {
        "id":            row_id,
        "webinar_id":    webinar_id,
        "type":          email_type,
        "subject":       subject,
        "template_name": tname,
        "status":        None,
        "template_id":   None,
        "audit":         {},
        "error":         None,
    }

    logger.info("── row id=%-5d  %s", row_id, tname)

    try:
        # Step 1: HTML Improvement Agent (cleanup + Liquid substitution)
        cleaned_html, audit = run_html_agent(html, filename, row_id)
        result["audit"] = audit

        # Step 2: Guarantee content block tags are present (Python-side, Gemini-proof)
        cleaned_html = enforce_content_blocks(cleaned_html, row_id)

        # Debug: save cleaned HTML to file for inspection
        if debug:
            debug_path = f"debug_row_{row_id}.html"
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(cleaned_html)
            logger.info("[row %d] Debug HTML saved to %s", row_id, debug_path)

        # Step 3: Image URL guard
        assert_images_intact(html, cleaned_html, row_id)

        # Step 3: Braze upsert
        if dry_run:
            logger.info("[row %d] DRY RUN — skipping Braze write", row_id)
            result["status"] = "dry_run"
        elif tname in existing_templates:
            tid = existing_templates[tname]
            update_template(tid, tname, subject, cleaned_html)
            logger.info("[row %d] Updated  template_id=%s", row_id, tid)
            result.update(status="updated", template_id=tid)
            time.sleep(BRAZE_RATE_SLEEP)
        else:
            tid = create_template(tname, subject, cleaned_html)
            existing_templates[tname] = tid
            logger.info("[row %d] Created  template_id=%s", row_id, tid)
            result.update(status="created", template_id=tid)
            time.sleep(BRAZE_RATE_SLEEP)

    except Exception as exc:
        logger.error("[row %d] FAILED: %s", row_id, exc, exc_info=True)
        result.update(status="error", error=str(exc))

    return result


def run_pipeline(
    csv_path: str,
    dry_run: bool = False,
    filter_ids: Optional[list[int]] = None,
    debug: bool = False,
) -> None:
    df = load_csv(csv_path)

    if filter_ids:
        df = df[df["id"].isin(filter_ids)]
        logger.info("Filtered to %d rows by --ids", len(df))

    existing_templates: dict[str, str] = {} if dry_run else get_existing_templates()

    results = []
    for _, row in df.iterrows():
        res = process_row(row, existing_templates, dry_run=dry_run, debug=debug)
        results.append(res)

    # ── Summary ──────────────────────────────────────────────────────────────
    summary  = pd.DataFrame(results)
    counts   = summary["status"].value_counts().to_dict()
    critical = [r for r in results if r.get("audit", {}).get("critical")]
    errors   = [r for r in results if r["status"] == "error"]

    logger.info("=" * 60)
    logger.info("MIGRATION COMPLETE — %d rows processed", len(results))
    for status, count in counts.items():
        logger.info("  %-10s %d", status, count)
    if critical:
        logger.warning("  Rows with critical audit issues: %d", len(critical))
        for r in critical:
            logger.warning("    row %-5s → %s", r["id"], r["audit"]["critical"])
    if errors:
        logger.error("  Rows with errors: %d", len(errors))
        for r in errors:
            logger.error("    row %-5s → %s", r["id"], r["error"])
    logger.info("=" * 60)

    out_path = "migration_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info("Audit trail written to %s", out_path)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate webinar emails to Braze")
    parser.add_argument("--csv",     required=True, help="Path to BigQuery CSV export")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run agent but skip all Braze API calls")
    parser.add_argument("--ids",     nargs="*", type=int,
                        help="Only process rows with these id values (for testing)")
    parser.add_argument("--debug",   action="store_true",
                        help="Save cleaned HTML to file before sending to Braze")
    args = parser.parse_args()

    run_pipeline(
        csv_path=args.csv,
        dry_run=args.dry_run,
        filter_ids=args.ids or None,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
