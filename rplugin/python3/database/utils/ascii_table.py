def ascii_table(headers: list, rows: list) -> list:
    lines = []
    lens = []
    num_rows = len(rows)
    num_columns = len(headers)
    for i in range(num_rows):
        while len(rows[i]) < num_columns:
            rows[i].append("")

        while len(rows[i]) > num_columns:
            rows[i] = rows[i][:-1]

    for i in range(num_columns):
        lens.append(len(max([x[i] for x in rows] + [headers[i]], key=lambda x: len(str(x)))))
    formats = []
    hformats = []
    for i in range(len(headers)):
        formats.append("%%-%ds" % lens[i])
        hformats.append("%%-%ds" % lens[i])
    pattern = " | ".join(formats)
    hpattern = " | ".join(hformats)
    separator = "+-" + "-+-".join(['-' * n for n in lens]) + "-+"
    lines.append(separator)
    lines.append("| " + hpattern % tuple(headers) + " |")
    lines.append(separator)
    for line in rows:
        lines.append("| " + pattern % tuple(t for t in line) + " |")

    if len(rows) == 0:
        pattern = "   ".join(formats)
        lines.append("| " + pattern % tuple(" " for t in headers) + " |")

    lines.append(separator)

    return lines
