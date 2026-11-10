#!/data/data/com.termux/files/usr/bin/bash
set -eu

base_dir="$HOME/.tfg-middleware/mosquitto"
tls_dir="$base_dir/tls"
ready_dir="/sdcard/Download/distributed-communication-platform"

pkg update -y
pkg install -y mosquitto
mkdir -p "$HOME/.termux" "$tls_dir" "$ready_dir"
printf '%s\n' 'allow-external-apps=true' > "$HOME/.termux/termux.properties"

test -r "$tls_dir/ca.crt"
test -r "$tls_dir/broker.crt"
test -r "$tls_dir/broker.key"
chmod 755 "$tls_dir"
chmod 644 "$tls_dir/ca.crt" "$tls_dir/broker.crt"
chmod 600 "$tls_dir/broker.key"
printf '%s\n' 'READY' > "$ready_dir/termux-ready"

echo 'Termux is provisioned. Close and reopen Termux, then grant the platform application permission to use Termux:RunCommand when Android requests it.'
