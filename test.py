

with open(f'//share1/DWH/_Dimension/dim_products.csv', 'r+', encoding="utf-8", newline=None) as f:
    income_csv_txt = f.read()
    parts_txt = income_csv_txt.split('|||')
    for i, t in enumerate(parts_txt[154::80]):
        parts_txt[154 + (80 * i)] = t.replace('\n', ' ')
    valid_csv_txt = '|||'.join(parts_txt)
    f.seek(0)
    f.write(valid_csv_txt)
    f.truncate()

