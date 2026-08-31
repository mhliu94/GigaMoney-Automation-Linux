#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sdk_dir="${ANDROID_SDK_ROOT:-${ANDROID_HOME:-/home/ubuntu/Android/Sdk}}"
platform_dir="${sdk_dir}/platforms/android-37.0"
build_tools_dir="${sdk_dir}/build-tools/36.0.0"
test_runner_jar="${platform_dir}/optional/android.test.runner.jar"
javac_bin="${JAVAC:-/home/ubuntu/.local/opt/android-studio/jbr/bin/javac}"
jar_bin="${JAR:-/home/ubuntu/.local/opt/android-studio/jbr/bin/jar}"
junit_jar="${JUNIT_JAR:-/home/ubuntu/.local/opt/android-studio/lib/junit4.jar}"
java_home="$(cd "$(dirname "${javac_bin}")/.." && pwd)"
export JAVA_HOME="${java_home}"
export PATH="${java_home}/bin:${PATH}"
build_dir="$(mktemp -d)"
trap 'rm -rf "${build_dir}"' EXIT

mkdir -p "${build_dir}/classes" "${build_dir}/dex" "${project_dir}/android"

"${javac_bin}" \
  --release 8 \
  -classpath "${platform_dir}/android.jar:${platform_dir}/uiautomator.jar:${test_runner_jar}:${junit_jar}" \
  -d "${build_dir}/classes" \
  "${project_dir}/android/src/gigamoney/ZeroIdleDump.java" \
  "${project_dir}/android/src/android/test/RepetitiveTest.java"

"${jar_bin}" --create --file "${build_dir}/classes.jar" -C "${build_dir}/classes" .
"${build_tools_dir}/d8" \
  --lib "${platform_dir}/android.jar" \
  --lib "${platform_dir}/uiautomator.jar" \
  --lib "${test_runner_jar}" \
  --lib "${junit_jar}" \
  --output "${build_dir}/dex" \
  "${build_dir}/classes.jar"
"${jar_bin}" --create \
  --file "${project_dir}/android/gigamoney-zero-idle-dump.jar" \
  -C "${build_dir}/dex" classes.dex

echo "Built ${project_dir}/android/gigamoney-zero-idle-dump.jar"
