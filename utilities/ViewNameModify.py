""" Please Export all Python Notebooks (.ipynb) to local folder and Use that folder in below path
 This Script is to identify the Common Temporary Views across the Notebooks
 Exception Cells will be printed on console, please check some views may have newline """
import os
path = 'C://jar//aa/'
entries = os.listdir(path)

import nbformat as nbf
ntbk_view_names = {}

updated_cells = {}
for entry in entries:
    data_file = path + entry
    # data_file = "C://jar//test/" + entry
    ntbk = nbf.read(data_file, nbf.NO_CONVERT)
    ntbk_nm = entry

    for cell in ntbk.cells:
        if cell.cell_type == "code":
            if 'TEMP VIEW' in cell['source'] or 'TEMPORARY VIEW' in cell['source']:
                import re
                s = cell['source']
                try:
                    vwnm = re.search(r'view(.*?) as', str(s).lower()).group(1).strip()
                    if vwnm in ntbk_view_names:
                        ntbk_view_names[vwnm].append(ntbk_nm.strip())
                    else:
                        # create a new array in this slot
                        ntbk_view_names[vwnm] = [ntbk_nm.strip()]
                except:
                    print(str(cell['source']))
                    print("No View Available in the Notebook   " + entry)
            # view_names.append(result.group(1).strip())

for key,value in ntbk_view_names.items():
    if len(value) >1:
        print(key,value)
