#!/usr/bin/env python3
"""Build a separate Magisk/CA ramdisk from the original Android 16 SDK image.

Run with work/proxy/venv/bin/python. This prepares an artifact; it does not stop
an emulator or change its AVD configuration. Use a stopped-emulator backup
before activating the result, as described in docs/proxy.md.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "work/proxy"
sys.path.insert(0, str(ROOT))
from gigamoney.adb import Adb
from gigamoney.config import load_settings

MAGISK_SHA256 = "e0d32d2123532860f97123d927b1bb86c4e08e6fd8a48bfc6b5bee0afae9ebd5"


def merge_cpio(data):
    """Merge concatenated newc archives, preserving entry metadata and ordering."""
    position = 0
    records = {}
    trailer = None
    while position < len(data):
        while position < len(data) and data[position] == 0:
            position += 1
        if position == len(data):
            break
        if data[position:position + 6] not in (b"070701", b"070702"):
            raise ValueError("Unexpected ramdisk archive format")
        fields = [int(data[position + 6 + i * 8:position + 14 + i * 8], 16) for i in range(13)]
        size, name_size = fields[6], fields[11]
        name_end = position + 110 + name_size
        if name_size < 1 or name_end > len(data) or data[name_end - 1] != 0:
            raise ValueError("Invalid cpio filename")
        name = data[position + 110:name_end - 1].decode()
        if name.startswith("/") or ".." in Path(name).parts:
            raise ValueError("Unsafe ramdisk entry")
        start = (name_end + 3) & ~3
        end = (start + size + 3) & ~3
        if end > len(data):
            raise ValueError("Truncated cpio entry")
        record = data[position:end]
        if name == "TRAILER!!!":
            trailer = record
        else:
            records[name] = record
        position = end
    if trailer is None or "init" not in records:
        raise ValueError("Ramdisk lacks init or an archive trailer")
    merged = b"".join(records.values()) + trailer
    return merged + b"\0" * (-len(merged) % 512)


def certificates():
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
    from mitmproxy.certs import CertStore

    directory = STATE / "ca"
    directory.mkdir(exist_ok=True, mode=0o700)
    CertStore.from_store(str(directory), "mitmproxy", 2048)
    if (directory / "api-cert.pem").exists():
        if not (directory / "api-cert-public.pem").exists():
            raise RuntimeError("Capture certificate public file is missing")
        return
    ca = x509.load_pem_x509_certificate((directory / "mitmproxy-ca-cert.pem").read_bytes())
    ca_key = serialization.load_pem_private_key((directory / "mitmproxy-ca.pem").read_bytes(), None)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(timezone.utc)
    certificate = (x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "*.lbkrs.com")]))
        .issuer_name(ca.subject).public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=365))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("*.lbkrs.com"), x509.DNSName("lbkrs.com")]), False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
        .sign(ca_key, hashes.SHA256()))
    public = certificate.public_bytes(serialization.Encoding.PEM)
    (directory / "api-cert-public.pem").write_bytes(public)
    (directory / "api-cert.pem").write_bytes(public + key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption()) + ca.public_bytes(serialization.Encoding.PEM))


def main():
    global STATE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config")
    parser.add_argument("--source-image", type=Path)
    parser.add_argument("--magisk-apk", type=Path, help="Official Magisk-v30.7.apk")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    settings = load_settings(args.config)
    STATE = settings.proxy_dir
    args.source_image = args.source_image or settings.resolve_path(settings.require("proxy.boot.sourceImage"))
    args.magisk_apk = args.magisk_apk or settings.resolve_path(settings.require("proxy.boot.magiskApk"))
    args.output = args.output or (settings.resolve_path(settings.get("proxy.boot.outputDirectory"))
                                  if settings.get("proxy.boot.outputDirectory") else STATE / "image")
    os.umask(0o077)
    STATE.mkdir(exist_ok=True, parents=True, mode=0o700)
    STATE.chmod(0o700)
    if args.output.exists():
        raise RuntimeError("Output already exists; use a new --output directory to rebuild")
    if hashlib.sha256(args.magisk_apk.read_bytes()).hexdigest() != MAGISK_SHA256:
        raise RuntimeError("Magisk APK does not match the reviewed 30.7 release")
    source = args.source_image.resolve()
    if (source / "ramdisk.img").read_bytes()[:4] != bytes.fromhex("02214c18"):
        raise RuntimeError("Expected the original LZ4 Android SDK ramdisk")
    adb = Adb(settings)
    if adb.shell("getprop", "ro.build.version.sdk").strip() != "36" or adb.shell("getprop", "ro.product.cpu.abi").strip() != "x86_64":
        raise RuntimeError("This preparation procedure was validated on Android API 36 x86_64 only")
    certificates()
    build = STATE / "boot-build"
    build.mkdir(exist_ok=True, mode=0o700)
    remote = "/data/local/tmp/gigamoney-proxy-boot"
    adb.shell("mkdir", "-p", remote)

    def push(path, name):
        subprocess.run([adb.executable, "-s", adb.serial, "push", str(path), remote + "/" + name],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def pull(name, destination):
        subprocess.run([adb.executable, "-s", adb.serial, "pull", remote + "/" + name, str(destination)],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    with zipfile.ZipFile(args.magisk_apk) as archive:
        for original, name in [("lib/x86_64/libmagiskboot.so", "magiskboot"),
                               ("lib/x86_64/libmagiskinit.so", "magiskinit"),
                               ("lib/x86_64/libmagisk.so", "magisk"),
                               ("lib/x86_64/libinit-ld.so", "init-ld"), ("assets/stub.apk", "stub.apk")]:
            path = build / name
            path.write_bytes(archive.read(original))
            push(path, name)
    push(source / "ramdisk.img", "ramdisk.img")
    adb.shell(f"cd {remote} && chmod 755 magiskboot && ./magiskboot decompress ramdisk.img ramdisk.cpio")
    pull("ramdisk.cpio", build / "ramdisk.cpio")
    (build / "merged.cpio").write_bytes(merge_cpio((build / "ramdisk.cpio").read_bytes()))
    push(build / "merged.cpio", "ramdisk.cpio")
    certificate_hash = subprocess.check_output(["openssl", "x509", "-in", str(STATE / "ca/mitmproxy-ca-cert.pem"),
                                                "-subject_hash_old", "-noout"], text=True).strip()
    (STATE / "certificate-hash.txt").write_text(certificate_hash + "\n")
    (build / "ca.sh").write_text((ROOT / "scripts/proxy_ca.sh").read_text().replace("__CA_HASH__", certificate_hash))
    push(build / "ca.sh", "gigamoney-proxy-ca.sh")
    push(ROOT / "scripts/proxy_ca.rc", "proxy-ca.rc")
    push(STATE / "ca/mitmproxy-ca-cert.pem", "gigamoney-proxy-ca.pem")
    patch = '''set -eu
cd /data/local/tmp/gigamoney-proxy-boot
cp ramdisk.cpio ramdisk.cpio.orig
./magiskboot compress=xz magisk magisk.xz
./magiskboot compress=xz init-ld init-ld.xz
./magiskboot compress=xz stub.apk stub.xz
printf 'KEEPVERITY=true\\nKEEPFORCEENCRYPT=true\\nRECOVERYMODE=false\\nVENDORBOOT=false\\n' > config
export KEEPVERITY=true KEEPFORCEENCRYPT=true
./magiskboot cpio ramdisk.cpio \\
"add 0750 init magiskinit" "mkdir 0750 overlay.d" "mkdir 0750 overlay.d/sbin" \\
"add 0644 overlay.d/sbin/magisk.xz magisk.xz" "add 0644 overlay.d/sbin/init-ld.xz init-ld.xz" \\
"add 0644 overlay.d/sbin/stub.xz stub.xz" "add 0644 overlay.d/proxy-ca.rc proxy-ca.rc" \\
"add 0755 overlay.d/sbin/gigamoney-proxy-ca.sh gigamoney-proxy-ca.sh" \\
"add 0644 overlay.d/sbin/gigamoney-proxy-ca.pem gigamoney-proxy-ca.pem" \\
"patch" "backup ramdisk.cpio.orig" "mkdir 000 .backup" "add 000 .backup/.magisk config"
./magiskboot compress=lz4_legacy ramdisk.cpio ramdisk-patched.img
'''
    adb.shell(patch)
    args.output.mkdir(parents=True, mode=0o700)
    for path in source.iterdir():
        if path.name != "ramdisk.img":
            (args.output / path.name).symlink_to(path)
    pull("ramdisk-patched.img", args.output / "ramdisk.img")
    print(f"Prepared {args.output.resolve()}. Original SDK image unchanged.")


if __name__ == "__main__":
    main()
