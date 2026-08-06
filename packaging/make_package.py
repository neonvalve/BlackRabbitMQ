#!/usr/bin/env python3
"""Сборка ZIP-пакета внешней компоненты для 1С.

1С ожидает архив с MANIFEST.XML в корне, перечисляющим бинарники под каждую
платформу. Манифест описывает ровно те файлы, что лежат в архиве: если
объявить отсутствующий компонент, платформа откажется подключать компоненту.

Скрипт используется в двух местах:
  * цель CMake `package1c` — пакет с бинарником текущей платформы;
  * релизный workflow — один пакет из артефактов Linux и Windows.

Пример:
    make_package.py --version 0.3.0 --output dist/BlackRabbitMQ-0.3.0.zip \\
        --component Linux:x86_64:build/libBlackRabbitMQ.so \\
        --component Windows:x86_64:build/Release/BlackRabbitMQ.dll
"""

import argparse
import os
import sys
import zipfile
from xml.sax.saxutils import quoteattr

# Значения, которые понимает 1С в атрибутах <component>.
VALID_OS = ("Windows", "Linux", "MacOS")
VALID_ARCH = ("i386", "x86_64", "arm", "arm64")

MANIFEST_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<bundle xmlns="http://v8.1c.ru/8.2/addin/bundle" name={name}>
{components}
</bundle>
"""


def parse_component(spec):
    """`OS:arch:path` → (os, arch, path). Ошибку показываем сразу и внятно."""
    parts = spec.split(":")
    if len(parts) < 3:
        raise argparse.ArgumentTypeError(
            "компонент задаётся как OS:arch:path, получено: %r" % spec)
    system, arch, path = parts[0], parts[1], ":".join(parts[2:])
    if system not in VALID_OS:
        raise argparse.ArgumentTypeError(
            "неизвестная ОС %r, допустимо: %s" % (system, ", ".join(VALID_OS)))
    if arch not in VALID_ARCH:
        raise argparse.ArgumentTypeError(
            "неизвестная архитектура %r, допустимо: %s" % (arch, ", ".join(VALID_ARCH)))
    return system, arch, path


def archive_names(components):
    """Имя каждого бинарника внутри архива.

    Windows x64 и x86 собираются в одинаковый BlackRabbitMQ.dll, а в одном
    архиве имена обязаны различаться. Совпадающие разводим суффиксом
    архитектуры; уникальные оставляем как есть, чтобы не ломать существующие
    пакеты.
    """
    counts = {}
    for _, _, path in components:
        base = os.path.basename(path)
        counts[base] = counts.get(base, 0) + 1

    names = []
    for _, arch, path in components:
        base = os.path.basename(path)
        if counts[base] > 1:
            stem, ext = os.path.splitext(base)
            names.append("%s-%s%s" % (stem, arch, ext))
        else:
            names.append(base)
    return names


def build_manifest(name, components, names):
    lines = []
    for (system, arch, _), arcname in zip(components, names):
        lines.append(
            '    <component os={os} path={path} type="native" arch={arch}/>'.format(
                os=quoteattr(system),
                path=quoteattr(arcname),
                arch=quoteattr(arch)))
    return MANIFEST_TEMPLATE.format(name=quoteattr(name), components="\n".join(lines))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Сборка ZIP-пакета внешней компоненты 1С")
    parser.add_argument("--name", default="BlackRabbitMQ", help="имя компоненты в манифесте")
    parser.add_argument("--version", required=True, help="версия (для имени архива и вывода)")
    parser.add_argument("--output", required=True, help="путь к создаваемому ZIP")
    parser.add_argument("--component", action="append", required=True, type=parse_component,
                        metavar="OS:arch:path", help="бинарник платформы, можно повторять")
    args = parser.parse_args(argv)

    missing = [path for _, _, path in args.component if not os.path.isfile(path)]
    if missing:
        parser.error("файлы не найдены: %s" % ", ".join(missing))

    names = archive_names(args.component)
    if len(set(names)) != len(names):
        parser.error("не удалось развести имена бинарников в архиве: %s"
                     % ", ".join(sorted(names)))

    manifest = build_manifest(args.name, args.component, names)

    output_dir = os.path.dirname(os.path.abspath(args.output))
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with zipfile.ZipFile(args.output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("MANIFEST.XML", manifest)
        for (_, _, path), arcname in zip(args.component, names):
            archive.write(path, arcname)

    print("%s (%s)" % (args.output, ", ".join(
        "%s/%s: %s" % (s, a, n)
        for (s, a, _), n in zip(args.component, names))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
