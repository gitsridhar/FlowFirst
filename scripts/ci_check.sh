#!/usr/bin/env bash
# FlowFirst quality gate. Run locally before committing; CI runs the same script.
#   ./scripts/ci_check.sh            all checks
#   ./scripts/ci_check.sh --quick    skip the HTTP handler tests (they start a subprocess)
# Needs no cluster, no network and no .env. Exit code 0 = every check passed.
#
# Checks: (1) Python 3.9 syntax of every .py   (2) bash -n on every .sh   (3) ruff (bug-finding rules only)
#         (4) pytest   (5) lock files in step with their .in files   (6) no secret-looking files tracked
set -uo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-python3}"
QUICK=0; [ "${1:-}" = "--quick" ] && QUICK=1
FAILED=()
step() { printf '\n== %s\n' "$1"; }
fail() { FAILED+=("$1"); printf 'FAIL: %s\n' "$1"; }

step "1. Python syntax (3.9 grammar, the RHEL 9 default)"
"$PY" - <<'PYEOF' || fail "python syntax"
import ast, pathlib, sys
bad = 0
for p in sorted(pathlib.Path(".").rglob("*.py")):
    if any(part in (".git", ".venv", "node_modules", "pki") for part in p.parts):
        continue
    try:
        ast.parse(p.read_text(), filename=str(p), feature_version=(3, 9))
    except SyntaxError as e:
        print(f"{p}:{e.lineno}: {e.msg}"); bad += 1
print("python files with errors:", bad)
sys.exit(1 if bad else 0)
PYEOF

step "2. bash -n on every shell script"
bad=0
while IFS= read -r f; do
    bash -n "$f" 2>&1 || { echo "syntax error: $f"; bad=1; }
done < <(find . -name '*.sh' -not -path './.git/*' -not -path './.venv/*' | sort)
[ "$bad" = 0 ] && echo "all shell scripts parse" || fail "shell syntax"

step "3. ruff (bug-finding rules, see ruff.toml)"
if "$PY" -m ruff --version >/dev/null 2>&1; then
    "$PY" -m ruff check --no-cache src scripts tests || fail "ruff"
else
    echo "ruff not installed: pip install --require-hashes -r requirements.txt -r requirements-dev.txt"; fail "ruff missing"
fi

step "4. pytest"
if "$PY" -m pytest --version >/dev/null 2>&1; then
    if [ "$QUICK" = 1 ]; then
        "$PY" -m pytest tests -q -p no:cacheprovider --ignore=tests/test_process1_http.py || fail "pytest"
    else
        "$PY" -m pytest tests -q -p no:cacheprovider || fail "pytest"
    fi
else
    echo "pytest not installed"; fail "pytest missing"
fi

step "5. lock files match their .in files"
for pair in "requirements.in:requirements.txt" "requirements-dev.in:requirements-dev.txt"; do
    src="${pair%%:*}"; lock="${pair##*:}"
    [ -s "$lock" ] || { fail "$lock missing"; continue; }
    # every direct dependency named in the .in file must appear pinned in the lock
    while IFS= read -r name; do
        grep -qiE "^${name//_/[-_]}==" "$lock" || { echo "$name from $src is not pinned in $lock (run scripts/update_requirements.sh)"; fail "lock $lock"; }
    done < <(grep -vE '^\s*(#|-)|^\s*$' "$src" | sed -E 's/[<>=!~ ;].*//')
    grep -q -- '--hash=sha256:' "$lock" || fail "$lock has no hashes"
done
echo "lock check done"

step "6. no key or environment files tracked by git"
if git rev-parse --git-dir >/dev/null 2>&1; then
    leaked="$(git ls-files | grep -E '(^|/)(\.env|.*\.key|.*\.pem|.*\.p12)$' || true)"
    [ -z "$leaked" ] && echo "none" || { echo "$leaked"; fail "secret files tracked"; }
else
    echo "not a git checkout: skipped"
fi

echo
if [ "${#FAILED[@]}" -eq 0 ]; then echo "ALL CHECKS PASSED"; exit 0; fi
printf 'FAILED: %s\n' "${FAILED[@]}"; exit 1
