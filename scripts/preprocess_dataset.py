#!/usr/bin/env python3

import csv
import re
import sys
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile


NS_MAIN = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
NS_REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
DATE_STYLE_IDS = {14, 15, 16, 17, 18, 19, 20, 21, 22, 45, 46, 47}


def column_index_from_ref(cell_ref):
    letters = re.match(r"[A-Z]+", cell_ref)
    if not letters:
        raise ValueError(f"Unexpected cell reference: {cell_ref}")
    result = 0
    for char in letters.group(0):
        result = result * 26 + (ord(char) - ord("A") + 1)
    return result - 1


def excel_date_to_string(value):
    from datetime import datetime, timedelta

    serial = float(value)
    base = datetime(1899, 12, 30)
    dt = base + timedelta(days=serial)
    if dt.time().strftime("%H:%M:%S") == "00:00:00":
        return dt.strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def load_shared_strings(workbook_file):
    try:
        with workbook_file.open("xl/sharedStrings.xml") as handle:
            root = ET.parse(handle).getroot()
    except KeyError:
        return []

    values = []
    for si in root.findall(f"{NS_MAIN}si"):
        parts = []
        for node in si.iter():
            if node.tag == f"{NS_MAIN}t" and node.text:
                parts.append(node.text)
        values.append("".join(parts))
    return values


def load_date_style_ids(workbook_file):
    try:
        with workbook_file.open("xl/styles.xml") as handle:
            root = ET.parse(handle).getroot()
    except KeyError:
        return set()

    numfmts = root.find(f"{NS_MAIN}numFmts")
    custom_date_numfmts = set()
    if numfmts is not None:
        for fmt in numfmts.findall(f"{NS_MAIN}numFmt"):
            num_fmt_id = int(fmt.attrib["numFmtId"])
            format_code = fmt.attrib.get("formatCode", "").lower()
            if any(token in format_code for token in ("yy", "dd", "mm", "hh", "ss")):
                custom_date_numfmts.add(num_fmt_id)

    style_ids = set()
    cell_xfs = root.find(f"{NS_MAIN}cellXfs")
    if cell_xfs is None:
        return style_ids

    for style_id, xf in enumerate(cell_xfs.findall(f"{NS_MAIN}xf")):
        num_fmt_id = int(xf.attrib.get("numFmtId", "0"))
        if num_fmt_id in DATE_STYLE_IDS or num_fmt_id in custom_date_numfmts:
            style_ids.add(style_id)
    return style_ids


def workbook_sheets(workbook_file):
    with workbook_file.open("xl/workbook.xml") as handle:
        workbook_root = ET.parse(handle).getroot()
    with workbook_file.open("xl/_rels/workbook.xml.rels") as handle:
        rels_root = ET.parse(handle).getroot()

    rel_map = {node.attrib["Id"]: node.attrib["Target"] for node in rels_root}
    sheets = []
    sheets_root = workbook_root.find(f"{NS_MAIN}sheets")
    if sheets_root is None:
        return sheets

    for sheet in sheets_root.findall(f"{NS_MAIN}sheet"):
        rel_id = sheet.attrib[f"{NS_REL}id"]
        target = rel_map[rel_id]
        if not target.startswith("xl/"):
            target = f"xl/{target}"
        sheets.append((sheet.attrib["name"], target))
    return sheets


def iter_sheet_rows(workbook_file, sheet_path, shared_strings, date_style_ids):
    with workbook_file.open(sheet_path) as handle:
        context = ET.iterparse(handle, events=("start", "end"))
        current_row = None

        for event, elem in context:
            if event == "start" and elem.tag == f"{NS_MAIN}row":
                current_row = []
            elif event == "end" and elem.tag == f"{NS_MAIN}c" and current_row is not None:
                cell_ref = elem.attrib.get("r", "A1")
                idx = column_index_from_ref(cell_ref)
                while len(current_row) <= idx:
                    current_row.append("")

                cell_type = elem.attrib.get("t")
                style_id = int(elem.attrib.get("s", "0"))
                value_node = elem.find(f"{NS_MAIN}v")
                inline_node = elem.find(f"{NS_MAIN}is")
                value = ""

                if cell_type == "s" and value_node is not None and value_node.text is not None:
                    value = shared_strings[int(value_node.text)]
                elif cell_type == "inlineStr" and inline_node is not None:
                    text_parts = [node.text for node in inline_node.iter(f"{NS_MAIN}t") if node.text]
                    value = "".join(text_parts)
                elif value_node is not None and value_node.text is not None:
                    raw = value_node.text
                    value = excel_date_to_string(raw) if style_id in date_style_ids else raw

                current_row[idx] = value
                elem.clear()
            elif event == "end" and elem.tag == f"{NS_MAIN}row":
                yield current_row or []
                current_row = None
                elem.clear()


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/online_retail_II.xlsx")
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("data/online_retail_II.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with ZipFile(input_path) as workbook_file, output_path.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        shared_strings = load_shared_strings(workbook_file)
        date_style_ids = load_date_style_ids(workbook_file)

        header_written = False
        header_width = 0

        for sheet_name, sheet_path in workbook_sheets(workbook_file):
            row_iter = iter_sheet_rows(workbook_file, sheet_path, shared_strings, date_style_ids)
            try:
                header = next(row_iter)
            except StopIteration:
                continue

            if not header_written:
                writer.writerow(header + ["SourceSheet"])
                header_width = len(header)
                header_written = True

            for row in row_iter:
                normalized = row[:header_width] + [""] * max(0, header_width - len(row))
                writer.writerow(normalized[:header_width] + [sheet_name])

    print(output_path)
    return 0


if __name__ == "__main__":
    main()
