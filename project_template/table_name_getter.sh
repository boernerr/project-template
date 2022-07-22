#!/usr/bin/env bash
################################################################################
#
#
# Table Name Getter
#
# Fetches the SQL tables declared by each sql query file.
#
# grep flags:
# -i case-insensitive
# -H return the file name
# -o print only what matches the regex pattern
# -P perl-style regex
#
# The pattern to match includes \K which is the short form of `(?<=pattern)`,
# a zero-width look-behind before the text you want to output. Basically, the
# capture group. The following `(?= as)` is a negative look-forward, so we are
# capturing the word between "table" and "as".
#
# `xargs -L 1 basename` simply strips away the directory path from the filename.
#
################################################################################

# Gives the full directory name of the script no matter where it's called from
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"

# Move to the root of the repo to execute
REPO_DIR="$SCRIPT_DIR/.."
cd $REPO_DIR

grep -iHoPr 'create table \K\w+(?= as)' ./sql_queries/* | xargs -L 1 basename > "$SCRIPT_DIR/feature_tables_created.txt"
grep -iHoPr 'from \K[\w\.]+' ./sql_queries/* | xargs -L 1 basename > "$SCRIPT_DIR/data_sources_used.txt"
