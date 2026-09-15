#!/usr/bin/env python3
"""python lang/check.py [code ...] - alignment / placeholder / tag check for
the aligned translation arrays against en.json."""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
en = json.load(open(os.path.join(HERE, "en.json"), encoding="utf-8"))
codes = sys.argv[1:] or [f[:-5] for f in os.listdir(HERE) if f.endswith(".json") and f not in ("en.json", "miniapp.json")]
TAGS = ("<b>", "</b>", "<i>", "</i>", "<code>", "</code>", "<blockquote>", "</blockquote>")
for code in codes:
    tr = json.load(open(os.path.join(HERE, code + ".json"), encoding="utf-8"))
    issues = []
    if len(tr) != len(en):
        issues.append(f"length {len(tr)} vs {len(en)}")
    for i, (a, b) in enumerate(zip(en, tr)):
        if not b:
            continue
        if a.count("{}") != b.count("{}"):
            issues.append(f"#{i} placeholders: {a[:50]!r} -> {b[:50]!r}")
        for t in TAGS:
            if a.count(t) != b.count(t):
                issues.append(f"#{i} tag {t}: {a[:50]!r} -> {b[:50]!r}")
    same = sum(1 for a, b in zip(en, tr) if a == b or not b)
    print(f"{code}: {len(tr)} entries, {same} left English, {len(issues)} issues")
    for x in issues[:30]:
        print("   ", x)
