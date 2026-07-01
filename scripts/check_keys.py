import re
with open(r'E:\3-IPBD\Tugas\project\bitcoin-volatility-ml\dashboard\app.py', encoding='utf-8') as f:
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
