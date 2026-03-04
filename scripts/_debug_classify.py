"""Debug classifier — shows why each email is/isn't classified as result_email."""
import json, re, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path

emails = json.loads(Path("data/emails_cache.json").read_text())

STRONG = {
    "sgpa","cgpa","marksheet","scorecard","grade card","grade sheet",
    "semester result","exam result","internal marks","grade points",
    "marks obtained","total marks","subject code","subject wise",
    "result declared","result published","university result",
    "cie marks","see marks","semester gpa","cumulative gpa",
}
WEAK = {
    "result","marks","grade","usn","pass","fail",
    "subject","semester","1ms","2ms","3ms","4ms",
    "backlog","arrear","revaluation","re-appear",
}
NEG = {
    "unsubscribe","opt-out","opt out","internship opportunity","job opening",
    "hiring","we are hiring","job alert","apply now","career","recruitment",
    "newsletter","promotional","discount","offer expires","your order",
    "invoice","meeting invite","zoom link","follow us on","social media",
    "linkedin","twitter","click to unsubscribe","marketing","sponsored",
    "limited time","act now","free trial","sign up",
}
USN_RE = re.compile(r"\b[1-4][a-z]{2}\d{2}[a-z]{2,4}\d{3}\b", re.I)

print(f"{'LABEL':25s} neg str  wk usn  SUBJECT")
print("-" * 100)
for e in emails:
    subj = (e.get("subject") or "").lower()
    body = (e.get("body") or e.get("snippet") or "").lower()
    text = subj + " " + body

    neg_h  = sum(1 for k in NEG    if k in text)
    str_h  = sum(1 for k in STRONG if k in text)
    wk_h   = sum(1 for k in WEAK   if k in text)
    has_usn = bool(USN_RE.search(text))
    subj_hit = any(k in subj for k in ("result","marks","marksheet","grade","sgpa","cgpa"))

    if neg_h >= 2:
        label = "other(neg)"
    elif str_h >= 1:
        label = "RESULT(strong)"
    elif has_usn and wk_h >= 2:
        label = "RESULT(usn+weak)"
    elif subj_hit and wk_h >= 2:
        label = "RESULT(subj+weak)"
    else:
        label = "other"

    # Show: result emails, anything with a USN, and anything with result/marks in subject
    if "RESULT" in label or has_usn or subj_hit:
        subj_display = e["subject"][:65]
        neg_hits_str = ", ".join(k for k in NEG if k in text)[:40]
        print(f"{label:25s}  {neg_h:3d} {str_h:3d} {wk_h:3d} {int(has_usn):3d}  {subj_display}")
        if neg_h >= 2:
            print(f"  >>> NEG matches: {neg_hits_str}")
        if str_h == 0 and (subj_hit or has_usn):
            strong_near = [k for k in STRONG if any(c in text for c in k.split())]
            missing = [k for k in ("sgpa","cgpa","marksheet","grade points","internal marks","marks obtained","total marks") if k not in text]
            print(f"  missing strong kw: {missing[:5]}")
