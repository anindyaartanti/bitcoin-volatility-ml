import re
from pathlib import Path
dashboard = Path(__file__).parent.parent / 'dashboard' / 'app.py'
with open(dashboard, encoding='utf-8') as f:
    content = f.read()
keys = re.findall(r"""key=["'](\w+)["']""", content)
counts = {}
for k in keys:
    counts[k] = counts.get(k, 0) + 1
dupes = {k: v for k, v in counts.items() if v > 1}
if dupes:
    print('[WARN] Duplicate keys found:')
    for k, v in dupes.items():
        print(f'  {k}: {v}x')
else:
    print('[OK] All Streamlit keys are unique')
print(f'Total keys: {len(keys)}, Unique: {len(counts)}')
