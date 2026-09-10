#!/system/bin/sh
# Run by the private emulator boot image before application processes start.
set -eu
proxy_ca_source="$1"
proxy_ca_store=/data/adb/gigamoney-proxy-ca/cacerts
proxy_ca_target=/apex/com.android.conscrypt/cacerts
[ -d "$proxy_ca_target" ] || proxy_ca_target=/system/etc/security/cacerts
mkdir -p "$proxy_ca_store"
cp -f "$proxy_ca_target"/* "$proxy_ca_store"/
cp "$proxy_ca_source/gigamoney-proxy-ca.pem" "$proxy_ca_store/__CA_HASH__.0"
chown -R 0:0 "$proxy_ca_store"
chmod 755 "$proxy_ca_store"
chmod 644 "$proxy_ca_store"/*
chcon -R u:object_r:system_file:s0 "$proxy_ca_store"
mount --bind "$proxy_ca_store" "$proxy_ca_target"
echo trusted > /data/local/tmp/gigamoney-proxy-ca.status
chmod 644 /data/local/tmp/gigamoney-proxy-ca.status
