#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2020-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import argparse
import string
import os
import shutil
import re
import subprocess
from pathlib import Path

class DebianFilesTemplate(string.Template):
    delimiter = '%'

def generate_changelog(scriptdir, outputdir, version, release, revision, codename):
    with open(os.path.join(scriptdir, 'changelog.template')) as f:
        changelog_template = f.read()
    s = DebianFilesTemplate(changelog_template)
    changelog_applied = s.substitute(version=version,
                                     release=release,
                                     revision=revision,
                                     codename=codename)
    with open(os.path.join(outputdir, 'changelog'), 'w', encoding='utf-8') as f:
        f.write(changelog_applied)

def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--version', action='store', help='specify version')
    arg_parser.add_argument('--release', action='store', help='specify release')
    arg_parser.add_argument('--revision', action='store', help='specify revision')
    arg_parser.add_argument('--codename', action='store', help='specify distribution codename')
    arg_parser.add_argument('--output-dir', action='store', default='debian/debian',
                            help='output directory')
    args = arg_parser.parse_args()
    outputdir = args.output_dir
    if os.path.exists(outputdir):
        shutil.rmtree(outputdir)
    shutil.copytree('dist/debian/debian', outputdir)
    scriptdir = os.path.dirname(__file__)
    generate_changelog(scriptdir, outputdir, args.version, args.release, args.revision, args.codename)


if __name__ == '__main__':
    main()
