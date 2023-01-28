from __future__ import annotations
import os.path
import pathlib
import re

import pandas

from openpyxl import load_workbook, Workbook
from openpyxl.worksheet.table import Table
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.worksheet import Worksheet

from openpyxl.utils import column_index_from_string, get_column_letter

from kb_package.utils.fdataset import DatasetFactory
from kb_package.tools import get_no_filepath, Cdict, replace_quoted_text


class ExcelFactory:
    MAX_COL = "XFD"

    def __init__(self, arg=get_no_filepath("excel_temp.xlsx"), **kwargs):
        self._path = get_no_filepath("excel_temp.xlsx")
        self._kwargs = kwargs
        self._got_modification = True

        if isinstance(arg, DatasetFactory):
            arg = arg.dataset
        if isinstance(arg, (str, pathlib.Path)):
            self._path = str(arg)
            if os.path.exists(self._path):
                if self._path.lower().endswith(".xlsm"):
                    kwargs["keep_vba"] = True
                wb = self.__load(path=arg, **kwargs)
            else:
                wb = Workbook()
        elif isinstance(arg, pandas.DataFrame):
            wb = Workbook()
            sh = wb.active
            for row in dataframe_to_rows(arg, index=False, header=True):
                sh.append(row)
        else:
            raise ValueError("Got bad value: %s" % (arg,))
        self._workbook: Workbook = wb
        self._tables_ref = []
        self._pivots_ref = []
        self._load_context()

    def __load(self, path=None, **kwargs):
        _kwargs = self._kwargs.copy()
        _kwargs.update(kwargs)

        path = self._path if not path else path
        self._workbook = load_workbook(path, **{k: v for k, v in _kwargs.items()
                                                if k in load_workbook.__code__.co_varnames})
        self._got_modification = False
        return self._workbook

    def pivot(self, name, get_idx=True, **kwargs):
        for pivot in self._pivots_ref:
            if name == pivot.name:
                if get_idx:
                    return pivot, self._workbook[pivot.sheet]._pivots[pivot.index]
                return pivot, self._workbook[pivot.sheet]._pivots[pivot.index]
        if "default" in kwargs:
            return kwargs["default"]
        raise ValueError("pivot named %s not exists" % name)

    def _get_ref_sheet_from(self, ref, sheet=None):
        if ref in self.tables_name:
            idx, table = self.table(ref, get_idx=True, default=-1)
            ref = table.ref
            sheet = self._tables_ref[idx].sheet
        ref = self._parse_ref(ref)
        if sheet is None:
            sheet = ref.get("sheet")
        if sheet is None:
            sheet = self._workbook.active
        elif not isinstance(sheet, Worksheet):
            sheet = self.sheet(sheet)
        return ref, sheet

    def ref_to_dataframe(self, ref, sheet=None):
        ref, sheet = self._get_ref_sheet_from(ref, sheet)

        columns = list(sheet.iter_rows(
            min_row=ref.start.line, max_row=ref.start.line,
            min_col=ref.start.col_id, max_col=ref.end.col_id,
            values_only=True))[0]
        dataset = DatasetFactory([
            list(d) for d in sheet.iter_rows(min_row=ref.start.line + 1, max_row=ref.end.line,
                                             min_col=ref.start.col_id, max_col=ref.end.col_id, values_only=True)
        ], columns=columns).dataset

        return dataset

    @property
    def tables_name(self):
        return [d.name for d in self._tables_ref]

    def _load_context(self):
        # define here all context
        _tables_ref = []
        wb = self._workbook
        _pivots_ref = []
        for sheet in wb.sheetnames:
            nb_temp = len(_tables_ref)
            sh = wb[sheet]
            _pivots_ref.extend([Cdict(name=pivot.name, sheet=sheet, ref=pivot.location.ref, index=index)
                                for index, pivot in enumerate(sh._pivots)])
            _tables_ref.extend([Cdict(name=k, sheet=sheet, idx=i + nb_temp) for i, k in enumerate(sh.tables.keys())])
        self._tables_ref = _tables_ref
        self._pivots_ref = _pivots_ref

    def table(self, name, get_idx=False, **kwargs):
        if name not in self.tables_name:
            if "default" in kwargs:
                return kwargs["default"]
            raise ValueError("Table %s don't exists" % name)
        for table in self._tables_ref:
            if name == table.name:
                if get_idx:
                    return table.idx, self._workbook[table.sheet].tables[name]
                return self._workbook[table.sheet].tables[name]
        if "default" in kwargs:
            return kwargs["default"]
        raise ValueError("Table %s don't exists" % name)

    def __enter__(self):
        return self._workbook

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._load_context()
        self.save(force=True)

    def sheet(self, name, create_if_not_exists=True, sheet_index=None) -> Worksheet:
        if create_if_not_exists and name not in self._workbook.sheetnames:
            self._workbook.create_sheet(name, index=sheet_index)
        return self._workbook[name]

    def save(self, path=None, force=False):
        path = path or self._path
        if not force and not self._got_modification:
            return path
        self._workbook.save(filename=path)
        self._got_modification = False
        return path

    def clear_content(self, ref, sheet=None):
        ref, sheet = self._get_ref_sheet_from(ref, sheet)
        _ref = ref.start.address
        if "end" in ref:
            _ref += ":" + ref.end.address
        else:
            sheet[_ref].value = None
            return
        for row in sheet[_ref]:
            for cell in row:
                cell.value = None

    @staticmethod
    def _parse_ref(ref: str):
        unquoted_ref, quoted_text_dict = replace_quoted_text(ref, quotes="'")
        unquoted_ref = unquoted_ref.split("!")
        if len(unquoted_ref) == 2:
            sheet = unquoted_ref[0]
            ref = unquoted_ref[1]
            for k in quoted_text_dict:
                sheet = sheet.replace(k, quoted_text_dict[k])
        else:
            sheet = None
        ref = ref.replace("$", "")
        res = Cdict()
        if sheet is not None:
            res["sheet"] = sheet
        for ref, part in zip(ref.upper().split(":"), ["start", "end"]):
            _, col, line, _ = re.split(r"^([A-Z]+)(\d+)$", ref)
            res[part] = Cdict({"col": col, "line": int(line), "address": ref,
                               "col_id": column_index_from_string(col)})

        return res

    def insert_data_to_table(self, data, table_name, sheet=None):
        if hasattr(data, "shape") and not getattr(data, "shape")[0]:
            return
        elif not hasattr(data, "shape") and not data:
            return

        if sheet is None:
            sheet = self._workbook.active
        elif not isinstance(sheet, Worksheet):
            sheet = self.sheet(sheet)
        table: Table = sheet.tables[table_name]

        data = DatasetFactory(data, column=table.column_names).dataset
        ref = self._parse_ref(table.ref)
        current_line = ref.end.line + 1
        for row in dataframe_to_rows(data, index=False, header=False):
            current_col = ref.start.col_id
            for value in row:
                sheet.cell(row=current_line, column=current_col).value = value
                current_col += 1
            current_line += 1
        ref = ref.start.col + str(ref.start.line) + ":" + ref.end.col + str(current_line - 1)
        table.ref = ref
        self._got_modification = True

    def create_table(self, data, ref="A1", sheet=None, table_name=None, header=True,
                     cols_name=None, replace_if_exists=2):
        """
            replace_if_exists: int or bool, bool:: if False and the table_name where exists raise Error; True ==> 1
                                int: 0 -> raise error if table_name where exists
                                     1 -> Replace table content if table where exists
                                     2 -> Adapte table_name if table_name where exists
        """

        if hasattr(data, "shape") and not getattr(data, "shape")[0]:
            return
        elif not hasattr(data, "shape") and not data:
            return
        # Ref calculation
        ref = self._parse_ref(ref)

        if sheet is None:
            sheet = ref.get("sheet")
        # table calculation
        if replace_if_exists == 2:
            if table_name is None:
                table_name = "Tableau"
            new_table_name = table_name
            iter_try = 1
            while new_table_name in self.tables_name:
                new_table_name = table_name + "_" + str(iter_try)
                iter_try += 1
            table_name = new_table_name
        elif table_name is None:
            raise ValueError("table_name arg was omitted")
        replace_if_exists = bool(replace_if_exists)
        table = self.table(table_name, get_idx=True, default=-1)
        if table != -1:
            idx, table = table
            if replace_if_exists:
                self._workbook[self._tables_ref[idx].sheet].tables.pop(table_name)
                last_sheet = self._tables_ref[idx].sheet
                self.clear_content(table.ref, sheet=last_sheet)
                if sheet is None:
                    sheet = last_sheet
            else:
                raise ValueError("Table %s already exists" % table_name)
        if not header and cols_name is None:
            if isinstance(data, dict):
                pass
            elif isinstance(data, list) and isinstance(data[0], list):
                cols_name = ["Colonne_" + str(i) for i in range(len(data[0]))]
        data = DatasetFactory(data, header=header, columns=cols_name).dataset
        if sheet is None:
            sheet = self._workbook.active
        elif not isinstance(sheet, Worksheet):
            sheet = self.sheet(sheet)
        current_line = ref.start.line
        current_col = 1
        for row in dataframe_to_rows(data, index=False, header=True):
            current_col = ref.start.col_id
            for value in row:
                sheet.cell(row=current_line, column=current_col).value = value
                current_col += 1
            current_line += 1

        _ref = ref.start.col + str(ref.start.line) + ":" + get_column_letter(current_col - 1)
        table_ref = _ref + str(current_line - 1)
        table = Table(displayName=table_name, ref=table_ref)
        sheet.add_table(table)
        # context
        self._load_context()
        self._got_modification = True
        return table_name

    def run_macro(self, macro, debug=False):
        path = self.save(force=True)
        import win32com.client as win32
        xlapp = win32.gencache.EnsureDispatch("Excel.Application")  # instantiate excel app
        if debug:
            xlapp.Visible = True
        wb = xlapp.Workbooks.Open(path)
        # xl.Application.Run('(classeur.xlsm!)?Module1.macro1("Jay")')
        xlapp.Application.Run(macro)
        wb.Save()
        xlapp.Application.Quit()
        self.__load()

    def create_macro(self, script: str | list | tuple, module_name=None, run=True, args=None, debug=False):
        global_run = run
        path = self.save(force=True)
        print("File is not saved")
        import win32com.client as win32
        xlapp = win32.gencache.EnsureDispatch("Excel.Application")  # instantiate excel app

        wb = xlapp.Workbooks.Open(path)
        if debug:
            xlapp.Visible = True
        xlmodule = wb.VBProject.VBComponents.Add(1)
        xlmodule.Name = module_name or "kb_Module"
        module_name = xlmodule.Name
        if not isinstance(script, (list, tuple)):
            scripts = [{"run": run, "script": script}]
        else:
            scripts = script
        for script in scripts:
            run = script.get("run", global_run)
            script = script.get("script")
            if not script.strip():
                continue
            xlmodule.CodeModule.AddFromString(script)
            # xl.Application.Run('(classeur.xlsm!)?Module1.macro1("Jay")')
            if run:
                res = re.search(r"^(sub|function)[ \t]+(\w+)[ \t]*(\([\w, \t]*\))$", script.strip().split("\n")[0],
                                flags=re.I | re.S)
                if res:
                    _type, name, _args = res.groups()
                    if _args and args:
                        if isinstance(args, dict):
                            for k in args:
                                pass
                        else:
                            # list, or tuple
                            name += "(" + ",".join([repr(x) for x in args]) + ")"

                    macro = module_name + "." + name
                    xlapp.Application.Run(macro)

        wb.Save()
        xlapp.Application.Quit()
        self.__load()

    def refresh_all(self, debug=False):
        path = self.save(force=True)
        import win32com.client as win32
        xlapp = win32.gencache.EnsureDispatch("Excel.Application")  # instantiate excel app
        if debug:
            xlapp.Visible = True
        wb = xlapp.Workbooks.Open(path)

        wb.RefreshAll()
        print("Saving ...")
        wb.Save()
        print("Finished")
        xlapp.Application.Quit()
        self.__load()


if __name__ == '__main__':
    excel = ExcelFactory()
