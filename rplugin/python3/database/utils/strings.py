def string_compose(target: str, pos: str, source: str) -> str:
    if source == '' or pos < 0:
        return target

    result = target[0:pos]
    if len(result) < pos:
        result += (' ' * pos - len(result))
    result += source
    result += ' ' + target[pos + len(source) + 1:]

    return result
