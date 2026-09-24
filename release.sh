#!/usr/bin/env bash
# Bumps the project version in pom.xml and prints the remaining steps to
# actually ship a release. This script only edits pom.xml - it never
# commits, pushes, or tags anything itself.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# Modern JDKs (24+) print a wall of harmless advance-notice warnings for
# things Maven's bundled libraries do today (native access, sun.misc.Unsafe,
# reflective final-field mutation) that a future JDK will restrict. Hide
# that noise on success; show it in full if the command actually fails.
run_quiet() {
  local err
  err="$(mktemp)"
  trap 'rm -f "$err"' RETURN
  if ! "$@" 2>"$err"; then
    cat "$err" >&2
    return 1
  fi
}

usage() {
  cat <<'EOF'
Usage: ./release.sh <major|minor|patch|X.Y.Z>

Bumps the version in pom.xml, ready to commit and release.

  major   1.2.9 -> 2.0.0
  minor   1.2.9 -> 1.3.0
  patch   1.2.9 -> 1.2.10
  X.Y.Z   set an explicit version, e.g. ./release.sh 2.0.0-rc1

Only pom.xml is changed. Nothing is committed, pushed, or tagged - the
script prints the remaining steps for you to run yourself.
EOF
}

if [ "$#" -ne 1 ]; then
  usage
  exit 1
fi

case "$1" in
  -h|--help)
    usage
    exit 0
    ;;
esac

if [ ! -f pom.xml ]; then
  echo "error: pom.xml not found - run this from the repo root." >&2
  exit 1
fi

if ! git diff --quiet -- pom.xml || ! git diff --cached --quiet -- pom.xml; then
  echo "error: pom.xml has uncommitted changes - commit or stash them first." >&2
  exit 1
fi

BUMP="$1"
case "$BUMP" in
  major|minor|patch) ;;
  *)
    if [[ ! "$BUMP" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z]+)*$ ]]; then
      echo "error: unknown argument '$BUMP'." >&2
      echo >&2
      usage >&2
      exit 1
    fi
    ;;
esac

if ! CURRENT="$(run_quiet ./mvnw -q -B help:evaluate -Dexpression=project.version -DforceStdout)"; then
  echo "error: failed to read the project version from pom.xml (see Maven output above)." >&2
  exit 1
fi

if [ "$BUMP" = "major" ] || [ "$BUMP" = "minor" ] || [ "$BUMP" = "patch" ]; then
  if [[ ! "$CURRENT" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    echo "error: pom.xml version '$CURRENT' isn't a plain MAJOR.MINOR.PATCH - bump it manually." >&2
    exit 1
  fi
  MAJOR="${BASH_REMATCH[1]}"
  MINOR="${BASH_REMATCH[2]}"
  PATCH="${BASH_REMATCH[3]}"
fi

case "$BUMP" in
  major) NEW="$((MAJOR + 1)).0.0" ;;
  minor) NEW="${MAJOR}.$((MINOR + 1)).0" ;;
  patch) NEW="${MAJOR}.${MINOR}.$((PATCH + 1))" ;;
  *) NEW="$BUMP" ;;
esac

if [ "$NEW" = "$CURRENT" ]; then
  echo "error: new version ($NEW) is the same as the current version - nothing to do." >&2
  exit 1
fi

echo "Current version: $CURRENT"
echo "New version:     $NEW"
echo

if ! run_quiet ./mvnw -q -B versions:set -DnewVersion="$NEW" -DgenerateBackupPoms=false; then
  echo "error: failed to update pom.xml to $NEW (see Maven output above)." >&2
  exit 1
fi
echo "pom.xml updated to $NEW"

cat <<EOF

Next steps:
  1. git checkout -b chore/bump-version-$NEW   (skip if you're already on a feature branch)
  2. git add pom.xml
  3. git commit -m "chore: bump version to $NEW"
  4. git push origin chore/bump-version-$NEW
  5. Open a PR to main and merge it
  6. On GitHub: Releases -> Draft a new release
       - Tag: v$NEW (target: main, after the PR above is merged)
       - Publish release
     This creates and pushes the v$NEW tag, which triggers release.yml to
     build the jar and attach it to this release automatically.

Tip: run './mvnw clean package -DskipUi' first if you want to sanity-check
the build locally before committing.
EOF
