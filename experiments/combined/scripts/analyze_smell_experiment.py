import csv
from collections import defaultdict
import glob

results_dir = './results'
files = sorted(glob.glob(f'{results_dir}/metrics_*.csv'))

print('='*75)
print('SS1 EXPERIMENT — MEASURED CPU CONSUMPTION (millicores) PER STAGE')
print('='*75)
print(f'{"Config":<25} {"Stage":<16} {"Avg CPU (m)":<14} {"Max CPU (m)":<14}')
print('-'*75)

for filepath in files:
    config = filepath.split('metrics_')[1].replace('.csv','')
    stage_data = defaultdict(list)
    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            stage_data[row['stage']].append(int(row['cpu_m']))
    for stage in ['ingestion','integration','cleaning','transformation','loading']:
        if stage in stage_data:
            vals = stage_data[stage]
            avg = sum(vals)/len(vals)
            mx = max(vals)
            print(f'{config:<25} {stage:<16} {avg:<14.1f} {mx:<14}')
    print()

print('='*75)
print('Note: 1000 millicores = 1 full CPU core')
print('      500m = 0.5 cores (right-sized baseline)')
print('      Values above 500m = smell-induced CPU amplification')
print('='*75)
print()
print('SUMMARY: CPU amplification caused by each smell')
print('-'*75)
baseline = {'ingestion':500.4,'integration':489.6,'cleaning':500.2,
            'transformation':500.5,'loading':473.9}
smell_configs = {
    'B_smell_ingestion':    'ingestion',
    'C_smell_integration':  'integration',
    'D_smell_cleaning':     'cleaning',
    'E_smell_transformation':'transformation',
    'F_smell_loading':      'loading',
}
print(f'{"Smell":<30} {"Stage affected":<18} {"Baseline":<12} {"With smell":<12} {"Increase%":<10}')
print('-'*75)
for filepath in files:
    config = filepath.split('metrics_')[1].replace('.csv','')
    if config not in smell_configs:
        continue
    affected_stage = smell_configs[config]
    stage_data = defaultdict(list)
    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            stage_data[row['stage']].append(int(row['cpu_m']))
    if affected_stage in stage_data:
        vals = stage_data[affected_stage]
        avg = sum(vals)/len(vals)
        base = baseline[affected_stage]
        increase = ((avg - base) / base) * 100
        print(f'{config:<30} {affected_stage:<18} {base:<12.1f} {avg:<12.1f} {increase:<10.1f}%')
print('='*75)
