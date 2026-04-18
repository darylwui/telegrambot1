#!/usr/bin/env python3
"""
Calls Claude API to generate fresh macro risks and watch items for today,
then overwrites macro_config.py. Run as a workflow step before the main
report scripts so each message gets current macro context.

Fails silently — if the API call fails, macro_config.py from the repo is
used unchanged and the main script still runs.
"""
import datetime
import json
import os
import re
import sys

import anthropic


def generate_macro():
    client = anthropic.Anthropic()
    today = datetime.date.today()

    prompt = f"""Today is {today.strftime('%B %d, %Y')}.

Generate a concise macro market outlook for US equity investors for this period.
Return ONLY a valid JSON object with exactly this structure — no markdown, no explanation:
{{
  "risks": [
    "sentence 1",
    "sentence 2",
    "sentence 3",
    "sentence 4"
  ],
  "watch": [
    "sentence 1",
    "sentence 2",
    "sentence 3",
    "sentence 4"
  ]
}}

risks: 4 key macro risk sentences, each 1-2 lines, factual and specific. Cover: Fed policy, geopolitical/trade risks, growth/recession signals, currency moves.
watch: 4 upcoming catalyst/event sentences with approximate dates. Cover: earnings, economic data releases, central bank meetings, policy decisions. Be specific with numbers and dates where known."""

    message = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    text = message.content[0].text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"\s*```$", "", text, flags=re.MULTILINE)
    return json.loads(text.strip())


def write_macro_config(macro):
    lines = ["MACRO = {"]
    lines.append('    "risks": [')
    for r in macro["risks"]:
        lines.append(f"        {json.dumps(r)},")
    lines.append("    ],")
    lines.append('    "watch": [')
    for w in macro["watch"]:
        lines.append(f"        {json.dumps(w)},")
    lines.append("    ],")
    lines.append("}")
    with open("macro_config.py", "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"macro_config.py updated ({len(macro['risks'])} risks, {len(macro['watch'])} watch items)")


def main():
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY not set — skipping macro update, using repo defaults.")
        return
    try:
        macro = generate_macro()
        write_macro_config(macro)
    except Exception as e:
        print(f"Warning: macro update failed ({e}) — using repo defaults.")


if __name__ == "__main__":
    main()
