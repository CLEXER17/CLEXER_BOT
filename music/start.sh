#!/bin/sh
# CLEXER Music entrypoint.
# PO-token provider (see Dockerfile): local HTTP service yt-dlp asks for a
# token on every YouTube request. MUSIC_NO_POT=1 turns it off.
if [ -z "$MUSIC_NO_POT" ] && [ -f /app/bgutil/server/build/main.js ]; then
  (cd /app/bgutil/server && node build/main.js --port 4416 >/tmp/pot.log 2>&1 &)
  sleep 2
  if curl -s -m 3 http://127.0.0.1:4416/ping >/dev/null 2>&1; then
    echo "[POT] provider up on :4416"; export MUSIC_POT_URL="http://127.0.0.1:4416"
  else
    echo "[POT] provider did not answer - continuing without it"; tail -3 /tmp/pot.log
  fi
fi If TS_AUTHKEY is set, bring up Tailscale in
# userspace mode (no root network access needed in a container) with a
# local HTTP proxy, route it through TS_EXIT_NODE (your phone / PC running
# Tailscale as an exit node) and point the service's MUSIC_PROXY at it.
if [ -n "$TS_AUTHKEY" ] && command -v tailscaled >/dev/null 2>&1; then
  echo "[TS] starting tailscaled (userspace)"
  tailscaled --tun=userspace-networking --socks5-server=127.0.0.1:1055 --outbound-http-proxy-listen=127.0.0.1:1056 --state=/tmp/tailscaled.state >/tmp/tailscaled.log 2>&1 &
  sleep 3
  tailscale up --authkey="$TS_AUTHKEY" --hostname="clexer-music" --accept-routes ${TS_EXIT_NODE:+--exit-node="$TS_EXIT_NODE" --exit-node-allow-lan-access} || echo "[TS] up failed - check TS_AUTHKEY / TS_EXIT_NODE"
  tailscale status 2>/dev/null | head -5
  export MUSIC_PROXY="http://127.0.0.1:1056"
  # the route to the exit node takes a few seconds to come up - wait for the
  # proxy to actually reach the internet before the service starts using it
  i=0; ok=""
  while [ $i -lt 12 ]; do
    ip=$(curl -s -m 8 -x http://127.0.0.1:1056 https://api.ipify.org 2>/dev/null)
    if [ -n "$ip" ]; then ok="$ip"; break; fi
    i=$((i+1)); sleep 5
  done
  if [ -n "$ok" ]; then
    echo "[TS] proxy ready - YouTube will see $ok (exit node ${TS_EXIT_NODE:-<none>})"
  else
    echo "[TS] proxy NOT passing traffic after 60s - exit node ${TS_EXIT_NODE:-<none>} unreachable?"
    tailscale ping -c 3 "$TS_EXIT_NODE" 2>&1 | tail -3
    tailscale status --json 2>/dev/null | grep -o '"ExitNodeStatus":{[^}]*}' | head -1
  fi
fi
exec python service.py
